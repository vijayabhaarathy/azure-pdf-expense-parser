import logging
import azure.functions as func
import pdfplumber
import pandas as pd
import io
import re
import os
from azure.storage.blob import BlobServiceClient, ContainerClient
from datetime import datetime

logging.info("🔥 Init.py loaded and running!")

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Constants
STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
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

        # 2. Connect to blob container
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = ContainerClient.from_connection_string(
            STORAGE_CONNECTION_STRING,
            container_name=INPUT_CONTAINER
        )

        # 3. Loop through all blobs (files)
        all_transactions = []

        for blob in container_client.list_blobs():
            if blob.name.endswith(".pdf"):
                blob_client = container_client.get_blob_client(blob)
                pdf_bytes = blob_client.download_blob().readall()
                pdf = pdfplumber.open(io.BytesIO(pdf_bytes))

                filename = blob.name.lower()
                if "axis" in filename:
                    all_transactions.extend(extract_axis_transactions(pdf, "Axis"))
                elif "4240" in filename:
                    all_transactions.extend(extract_hdfc_credit_transactions(pdf, "HDFC Diners"))
                elif "8069" in filename:
                    all_transactions.extend(extract_hdfc_credit_transactions(pdf, "HDFC Millennia"))
                elif "acct" in filename or "savings" in filename:
                    all_transactions.extend(extract_hdfc_savings_transactions(pdf))

                pdf.close()

        # 4. Convert to DataFrame
        df_combined = pd.DataFrame(all_transactions)
        df_combined["Transaction"] = df_combined["Transaction"].apply(
            lambda x: x[:60] + "..." if isinstance(x, str) and len(x) > 60 else x
        )
        df_combined["Amount"] = df_combined["Amount"].apply(
            lambda x: f"₹{x:,.2f}" if pd.notnull(x) else ""
        )
        df_combined.sort_values(by="Date", inplace=True)

        # 5. Save to Excel in memory
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='openpyxl')
        df_combined.to_excel(writer, index=False, sheet_name='Expenses')
        writer.close()
        output.seek(0)

        # 6. Upload Excel to Blob
        output_filename = f"expenses_consolidated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_blob_client = blob_service_client.get_blob_client(
            container=OUTPUT_CONTAINER,
            blob=output_filename
        )
        output_blob_client.upload_blob(output, overwrite=True)

        return func.HttpResponse(
            f"✅ PDF parsed and Excel saved as '{output_filename}' in container '{OUTPUT_CONTAINER}'",
            status_code=200
        )

    except Exception as e:
        logging.exception("❌ Error occurred:")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

# ----- Extraction Functions -----

def extract_axis_transactions(pdf, card_label):
    transactions = []
    for page in pdf.pages:
        for table in page.extract_tables():
            for row in table:
                if not row or len(row) < 9:
                    continue
                date_raw = row[0]
                if not re.match(r"\d{2}/\d{2}/\d{4}", date_raw or ""):
                    continue
                try:
                    date = pd.to_datetime(date_raw, dayfirst=True).date()
                except:
                    continue
                transaction = row[1].strip() if row[1] else ""
                merchant_category = row[4].strip() if row[4] else ""
                amount_text = row[7].replace(",", "") if row[7] else ""
                amount = None
                credit_debit = ""
                if "Dr" in amount_text:
                    amount = float(amount_text.replace("Dr", "").strip())
                    credit_debit = "Debit"
                elif "Cr" in amount_text:
                    amount = float(amount_text.replace("Cr", "").strip())
                    credit_debit = "Credit"
                transactions.append({
                    "Date": date,
                    "Month": date.strftime("%B"),
                    "Year": date.year,
                    "Card": card_label,
                    "Card Type": "Credit",
                    "Transaction": transaction,
                    "Amount": amount,
                    "Credit/Debit": credit_debit,
                    "Sub-Category": "",
                    "Category": ""
                })
    return transactions

def extract_hdfc_credit_transactions(pdf, card_label):
    transactions = []
    for page in pdf.pages:
        tables = page.extract_tables()
        for table in tables:
            header = table[0]
            if not any("Desc" in str(cell) for cell in header if cell):
                continue
            for row in table:
                if not row or len(row) < 2:
                    continue
                date_raw = row[0]
                if not re.match(r"\d{2}/\d{2}/\d{4}", date_raw or ""):
                    continue
                summary_keywords = [
                    "TOTAL DUES", "REWARDS", "PAYMENT DUE DATE", "STATEMENT DATE",
                    "MINIMUM AMOUNT", "CREDIT LIMIT", "CARD ENDING", "AVAILABLE CREDIT", "DUE DATE"
                ]
                if any(keyword in (row[1] or "").upper() for keyword in summary_keywords):
                    continue
                try:
                    date = pd.to_datetime(date_raw, dayfirst=True).date()
                except:
                    continue
                transaction = row[1].strip() if row[1] else ""
                potential_amounts = row[-2:]
                amount = None
                credit_debit = ""
                joined = " ".join(str(x) for x in potential_amounts if x)
                amt_match = re.search(r"([\d,]+\.\d{2})", joined)
                if amt_match:
                    amount = float(amt_match.group(1).replace(",", ""))
                    credit_debit = "Credit" if "Cr" in joined else "Debit"
                transactions.append({
                    "Date": date,
                    "Month": date.strftime("%B"),
                    "Year": date.year,
                    "Card": card_label,
                    "Card Type": "Credit",
                    "Transaction": transaction,
                    "Amount": amount,
                    "Credit/Debit": credit_debit,
                    "Sub-Category": "",
                    "Category": ""
                })
    return transactions

def extract_hdfc_savings_transactions(pdf):
    transactions = []
    prev_balance = None
    for page in pdf.pages:
        lines = page.extract_text().splitlines()
        current_txn = {}
        for line in lines:
            line = line.strip()
            date_match = re.match(r"^(\d{2}/\d{2}/\d{2,4})\s+(.*?)\s+([\d,]+\.\d{2})\s+([\d,]+\.\d{2})$", line)
            if date_match:
                if current_txn:
                    transactions.append(current_txn)
                    current_txn = {}
                date_raw, narration, amount_str, balance_str = date_match.groups()
                try:
                    date = pd.to_datetime(date_raw, dayfirst=True).date()
                    amount = float(amount_str.replace(",", ""))
                    balance = float(balance_str.replace(",", ""))
                except:
                    continue
                credit_debit = ""
                if prev_balance is not None:
                    credit_debit = "Credit" if balance > prev_balance else "Debit"
                prev_balance = balance
                current_txn = {
                    "Date": date,
                    "Month": date.strftime("%B"),
                    "Year": date.year,
                    "Card": "HDFC Savings",
                    "Card Type": "Savings",
                    "Transaction": narration.strip(),
                    "Amount": amount,
                    "Credit/Debit": credit_debit,
                    "Sub-Category": "",
                    "Category": ""
                }
            elif current_txn and "Transaction" in current_txn:
                current_txn["Transaction"] += " " + line.strip()
        if current_txn:
            transactions.append(current_txn)
    return transactions
