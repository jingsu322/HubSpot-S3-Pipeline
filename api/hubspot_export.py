import os
import json
import requests
import zipfile
import io
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import time
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class HubSpotExportAPI:
    def __init__(self, access_token):
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

    def start_export(self, payload):
        url = f"{self.base_url}/crm/v3/exports/export/async"
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        logging.info(f"HTTP Status: {response.status_code}, Response: {response.text}")
        if response.status_code != 202:
            logging.error(
                f"Failed to start export. HTTP Status: {response.status_code}, Response: {response.text}"
            )
        #     return None
        # task_id = response.json().get("id")
        task_id = json.loads(response.text).get("id")
        if not task_id:
            logging.error("Task ID not found in the response.")
            return None
        logging.info(f"Export started, task ID: {task_id}")
        return task_id

    def check_status(self, task_id):
        url = f"{self.base_url}/crm/v3/exports/export/async/tasks/{task_id}/status"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            logging.error(
                f"Failed to check status. HTTP Status: {response.status_code}, Response: {response.text}"
            )
            return None
        # status = json.loads(response.text).get("status")
        check_status = json.loads(response.text)
        status = check_status.get("status")
        result = check_status.get("result")
        if not status:
            logging.error("Status not found in the response.")
            return None
        # return status
        return status, result

    def download_export(self, download_url):
        response = requests.get(download_url, stream=True)
        if response.status_code != 200:
            logging.error(
                f"Failed to download export. HTTP Status: {response.status_code}, Response: {response.text}"
            )
            return None
        if "zip" in response.headers.get("Content-Type", ""):
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith(".csv"):
                        with zip_file.open(file_name) as csv_file:
                            logging.info(f"Extracted CSV file: {file_name}")
                            return pd.read_csv(csv_file)
        else:
            logging.info("Downloaded CSV file directly.")
            return pd.read_csv(io.StringIO(response.content.decode("utf-8")))


class DataProcessor:
    @staticmethod
    def create_rename_dict(columns):
        rename_dict = {}
        for col in columns:
            # Convert to lowercase and replace spaces with underscores
            new_name = col.lower().replace(" ", "_")
            # Add _pst if the column ends with _date
            if new_name.endswith("_date"):
                new_name += "_pst"
            # Remove special characters, keeping a-z, 0-9, and _
            new_name = re.sub(r"[^a-z0-9_]", "", new_name)
            rename_dict[col] = new_name
        return rename_dict

    @staticmethod
    def process_data(df):
        rename_dict = DataProcessor.create_rename_dict(df.columns)
        df.rename(columns=rename_dict, inplace=True)

        # Convert _id columns to string
        for col in df.columns:
            if col.endswith("_id") and df[col].dtype != "object":
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .astype("Int64")
                    .astype("str")
                )

        for col in df.columns:
            if col.startswith("number_of_") and df[col].dtype != "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Specific conversions
        if "buyer_product_list" in df.columns:
            df["buyer_product_list"] = df["buyer_product_list"].astype(str)
        if "annual_revenue" in df.columns:
            df["annual_revenue"] = (
                pd.to_numeric(df["annual_revenue"], errors="coerce")
                .fillna(0)
                .astype(int)
            )

        # Force null values to None
        df = df.replace({"<NA>": None})

        logging.info("Data processing completed.")
        return df


class S3Uploader:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def upload_to_s3(self, df, bucket_name, prefix, file_name):
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, bucket_name, f"{prefix}/{file_name}")
        logging.info(f"Data uploaded to S3: s3://{bucket_name}/{prefix}/{file_name}")


# if __name__ == "__main__":
#         # Load environment variables
#     load_dotenv("../.env", override=True)

#     # Initialize API and S3 uploader
#     ACCESS_TOKEN = os.getenv("HS_ACCESS_TOKEN")
#     export_api = HubSpotExportAPI(ACCESS_TOKEN)
#     uploader = S3Uploader(
#         os.getenv("AWS_ACCESS_KEY_ID"),
#         os.getenv("AWS_SECRET_ACCESS_KEY"),
#         os.getenv("AWS_REGION_NAME"),
#     )

#     # Define payload
#     payload = {
#         "exportType": "LIST",
#         "listId": 10807,
#         "exportName": "For Jing new leads",
#         "format": "CSV",
#         "language": "EN",
#         "objectType": "company",
#         "objectProperties": [
#             "name",
#             "domain",
#             "hubspot_owner_id", 
#             "createdate",
#             "phone",
#             "annualrevenue",
#             "buyer_product_list",
#             "seller_product_list",
#             "product_information",
#             "lifecyclestage",
#             "seller_lifecycle_stage",
#             "industry",
#             "hs_object_id",
#             "numberofemployees",
#             "hs_lastmodifieddate",
#         ],
#     }

#     task_id = export_api.start_export(payload)
#     if not task_id:
#         raise Exception("Failed to start export")

#     while True:
#         sleep_time = 60  # 30 seconds
#         time.sleep(sleep_time)  # Sleep before checking the status
#         status = export_api.check_status(task_id)[0]

#         if not status:
#             raise Exception("Failed to retrieve export status")
#         if status == "COMPLETE":
#             logging.info("Export completed!")
#             # download_url = status.get("result")
#             download_url = export_api.check_status(task_id)[1]
#             if not download_url:
#                 raise Exception("Download URL not found")
#             break
#         elif status in ["FAILED", "CANCELED"]:
#             logging.error(f"Export failed with status: {status}")
#             raise Exception(f"Export failed with status: {status}")
#         else:
#             logging.info(f"Export is {status}. Retrying in {sleep_time} seconds...")

#     df = export_api.download_export(download_url)
#     if df is None:
#         raise Exception("Failed to download export data")
#     logging.info("Data downloaded successfully.")

#     processed_df = DataProcessor.process_data(df)

#     uploader.upload_to_s3(
#         processed_df,
#         "io-data-lake",
#         "hubspot-export-data/new_leads_monthly",
#         "hubspot_new_leads_monthly.parquet",
#     )
#     logging.info("Data uploaded to S3 successfully.")