import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.hubspot_export import HubSpotExportAPI, DataProcessor, S3Uploader
from dotenv import load_dotenv
import os
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    # Load environment variables
    load_dotenv("../.env", override=True)

    # Initialize API and S3 uploader
    ACCESS_TOKEN = os.getenv("HS_ACCESS_TOKEN")
    export_api = HubSpotExportAPI(ACCESS_TOKEN)
    uploader = S3Uploader(
        os.getenv("AWS_ACCESS_KEY_ID"),
        os.getenv("AWS_SECRET_ACCESS_KEY"),
        os.getenv("AWS_REGION_NAME"),
    )

    # Define payload
    payload = {
        "exportType": "LIST",
        "listId": 10807,
        "exportName": "For Jing new leads",
        "format": "CSV",
        "language": "EN",
        "objectType": "company",
        "objectProperties": [
            "name",
            "domain",
            "hubspot_owner_id", 
            "createdate",
            "phone",
            "annualrevenue",
            "buyer_product_list",
            "seller_product_list",
            "product_information",
            "lifecyclestage",
            "seller_lifecycle_stage",
            "industry",
            "hs_object_id",
            "numberofemployees",
            "hs_lastmodifieddate",
        ],
    }

    task_id = export_api.start_export(payload)
    if not task_id:
        raise Exception("Failed to start export")

    while True:
        sleep_time = 60  # 30 seconds
        time.sleep(sleep_time)  # Sleep before checking the status
        status = export_api.check_status(task_id)[0]

        if not status:
            raise Exception("Failed to retrieve export status")
        if status == "COMPLETE":
            logging.info("Export completed!")
            # download_url = status.get("result")
            download_url = export_api.check_status(task_id)[1]
            if not download_url:
                raise Exception("Download URL not found")
            break
        elif status in ["FAILED", "CANCELED"]:
            logging.error(f"Export failed with status: {status}")
            raise Exception(f"Export failed with status: {status}")
        else:
            logging.info(f"Export is {status}. Retrying in {sleep_time} seconds...")

    df = export_api.download_export(download_url)
    if df is None:
        raise Exception("Failed to download export data")
    logging.info("Data downloaded successfully.")

    processed_df = DataProcessor.process_data(df)

    uploader.upload_to_s3(
        processed_df,
        "io-data-lake",
        "hubspot-export-data/new_leads_monthly",
        "hubspot_new_leads_monthly.parquet",
    )
    logging.info("Data uploaded to S3 successfully.")


if __name__ == "__main__":
    main()
