import logging
import azure.functions as func
import pdfplumber
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
from datetime import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Constants
STORAGE_CONNECTION_STRING = "<your-storage-connection-string>"
INPUT_CONTAINER = "expensesstatements"
OUTPUT_CONTAINER = "expensesexcel"

@app.route(route="PdfParserFunction")
def PdfParserFunction(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # 1. Get file name from query or JSON
        blob_name = req.params.get("blob")
        if not blob_name:
            try:
                req_body = req.get_json()
            except:
                return func.HttpResponse("Missing 'blob' parameter", status_code=400)
            blob_name = req_body.get("blob")

        if not blob_name:
            return func.HttpResponse("Please pass a blob name in the request", status_code=400)

        # 2. Connect to blob storage and download PDF
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=INPUT_CONTAINER, blob=blob_name)
        pdf_bytes = blob_client.download_blob().readall()

        # 3. Extract data from PDF
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            all_rows = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        all_rows.append(row)

        # 4. Convert to DataFrame
        df = pd.DataFrame(all_rows)
        df.dropna(how='all', inplace=True)
        df.columns = [f"Column{i+1}" for i in range(len(df.columns))]  # temporary

        # 5. Save as Excel in-memory
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='openpyxl')
        df.to_excel(writer, index=False, sheet_name='Expenses')
        writer.close()
        output.seek(0)

        # 6. Upload to output container
        output_filename = f"expenses_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_blob_client = blob_service_client.get_blob_client(container=OUTPUT_CONTAINER, blob=output_filename)
        output_blob_client.upload_blob(output, overwrite=True)

        # 7. Done
        return func.HttpResponse(
            f"✅ PDF parsed and Excel saved as '{output_filename}' in container '{OUTPUT_CONTAINER}'",
            status_code=200
        )

    except Exception as e:
        logging.exception("❌ Error occurred:")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
