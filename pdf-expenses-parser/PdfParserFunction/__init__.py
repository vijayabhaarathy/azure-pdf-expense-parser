import logging
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="PdfParserFunction")
def PdfParserFunction(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("✅ Azure Function triggered successfully.")
    return func.HttpResponse("✅ Function is ready to receive and process PDFs.", status_code=200)
