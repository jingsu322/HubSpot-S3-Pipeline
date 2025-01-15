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
        "listId": 10809,
        "exportName": "Company Table for DataLake",
        "format": "CSV",
        "language": "EN",
        "objectType": "company",
        "objectProperties": [
            "hs_object_id",
            "name",
            "domain",
            "hubspot_owner_id",
            "createdate",
            "phone",
            "notes_last_updated",
            "country",
            "state",
            "city",
            "industry",
            "annualrevenue",
            "annual_revenue_label",
            "hubspot_owner_assigneddate",
            "lifecyclestage",
            "seller_lifecycle_stage",
            "netsuite_company_id",
            "test2",
            "test",
            "seller_sales_rep2",
            "seller_rep_assigned_date",
            "account_manager__user_property_",
            "sell_form_lead_qualified_",
            "lead_qualified_",
            "lead_qualifier_2",
            "lead_qualified_date",
            "seller_first_contract_date",
            "seller_contract_expiration_date",
            "category",
            "customer_group",
            "numberofemployees",
            "num_associated_contacts",
            "num_contacted_notes",
            "call_center_rep_",
            "approved_online_registration",
            "date_of_first_registration__test_",
            "free_shipping_enrollment_date",
            "free_shipping_enrollment_date__new_jersey_",
            "free_shipping_disenrollment_date",
            "hs_analytics_num_page_views",
            "vip_seller",
            "web_address",
            "lead_source",
            "type",
            "division",
            "seller_program",
            "zip",
            "customer_territory",
            "hs_lastmodifieddate",
        ],
    }

    task_id = export_api.start_export(payload)
    if not task_id:
        raise Exception("Failed to start export")

    while True:
        sleep_time = 600  # 30 seconds
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
        "hubspot-export-data/all_companies",
        "all_companies.parquet",
    )

    logging.info("Data uploaded to S3 successfully.")


if __name__ == "__main__":
    main()
