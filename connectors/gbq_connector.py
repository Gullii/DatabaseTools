import logging

from google.cloud import bigquery
from google.oauth2 import service_account


class GoogleBigQueryConnection:
    def __init__(self, service_account_file_name: str, gbq_project_id: str):
        """
        :param service_account_file_name: Path to service account file
        :param gbq_project_id: Id of the Big Query Project
        """
        self.poject_id = gbq_project_id
        self.credentials = service_account.Credentials.from_service_account_file(
            service_account_file_name
        )

    def get_client(self):
        """
        Creates client based provided credentials
        :return: Bigquery Client
        """
        try:
            gbq_client = bigquery.Client(
                credentials=self.credentials, project=self.poject_id
            )
            logging.info("You are connected to Google BigQuery")
        except Exception as e:
            logging.exception("Error connecting to Google BigQuery: ")
            raise e
        else:
            return gbq_client

    def get_projectid(self) -> str:
        return self.poject_id

    def get_credentials(self):
        return self.credentials
