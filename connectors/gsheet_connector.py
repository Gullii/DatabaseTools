from oauth2client.service_account import ServiceAccountCredentials
import gspread
import logging


class GoogleSheetConnection:
    def __init__(self, gspread_json_name: str, gspread_scope: list):
        """
        Refer to https://developers.google.com/workspace/guides/auth-overview for google auth flow
        :param gspread_json_name: Path to gspread credentials
        :param gspread_scope: Defined scope for the app
        """
        self.gspread_json_name = gspread_json_name
        self.gspread_scope = gspread_scope

        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            gspread_json_name, gspread_scope
        )

    def get_client(self):
        try:
            gsheet_client = gspread.authorize(self.credentials)
        except Exception as e:
            logging.exception("Error connecting to Google Sheets: ")
            raise e
        else:
            return gsheet_client

    def get_scope(self):
        return self.gspread_scope

    def get_gspread_json(self):
        return self.gspread_json_name
