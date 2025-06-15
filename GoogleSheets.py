from __future__ import print_function
import os.path
import pickle
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from dotenv import load_dotenv
import os

load_dotenv()

class GoogleSheet:
    SPREADSHEET_ID = os.getenv('spreadsheet_id')
    SCOPES = [os.getenv("scopes")]
    service = None

    def __init__(self):
        creds = None
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('sheets', 'v4', credentials=creds)

    def appendRow(self, range_name, values):
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.SPREADSHEET_ID,
            range=range_name
        ).execute()

        values_existing = result.get('values', [])
        next_row = len(values_existing) + 1

        append_range = f"{range_name.split('!')[0]}!A{next_row}"

        body = {
            'values': values
        }
        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.SPREADSHEET_ID,
            range=append_range,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cells updated.")

    def readAllSheetsAsJson(self, output_file):
        # Get all sheet names
        sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', [])

        all_data = {}

        for sheet in sheets:
            sheet_name = sheet.get("properties", {}).get("title")
            range_name = f"{sheet_name}!A1:Z1000"  # Assuming data is within A1 to Z1000

            # Fetch data from each sheet
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.SPREADSHEET_ID,
                range=range_name
            ).execute()

            values = result.get('values', [])
            all_data[sheet_name] = values

        # Write the data to a JSON file
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(all_data, json_file, ensure_ascii=False, indent=4)

        print(f"Data from all sheets saved to {output_file}")


# Example usage
if __name__ == '__main__':
    sheet = GoogleSheet()
    # Read data from all sheets and save to a JSON file
    sheet.readAllSheetsAsJson('all_sheets_data.json')
