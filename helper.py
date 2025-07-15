from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os, csv, requests, io, json, gspread, gspread_dataframe, time                   
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from functools import reduce
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.discovery import Resource

def update(credentials_path:str, scope:list, token_location_id:str):
    flow = InstalledAppFlow.from_client_secrets_file(
        credentials_path, scopes=scope
    )
    creds = flow.run_local_server(port=0)
    drive_service = build('drive', 'v3', credentials=creds)
    
    json_creds = creds.to_json()
    bytes_file = io.BytesIO(json_creds.encode("utf-8"))
    
    
    gdrive_media = MediaIoBaseUpload(bytes_file, mimetype="text/plain")
    updated_file = drive_service.files().update(
    fileId=token_location_id,
    media_body=gdrive_media).execute()
    
    print(f"File '{updated_file.get('name')}' was updated.")
    print(f"Token = {json_creds}")

def load_credentials_from_gdrive(token_file_id:str, scopes:list):
    def download_file_from_google_drive(id):
        URL = "https://docs.google.com/uc?export=download"
        session = requests.Session()
        response = session.get(URL, params={'id': id}, stream=True)
        return response.content

    downloaded_file = io.BytesIO(download_file_from_google_drive(token_file_id))
    token_info_json = json.load(downloaded_file)
    creds = Credentials.from_authorized_user_info(token_info_json, scopes=scopes)

    return creds

def copy_excel_file(service:Resource, cleaned_data_folder_id:str):
    get_excel_query = ("mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
             "mimeType='application/vnd.ms-excel'")
    get_excel_query += " and trashed = false"
    file_list = service.files().list(
        q=f"'{cleaned_data_folder_id}' in parents and ({get_excel_query})",
        fields="files(id, name, mimeType)"
    ).execute()
    items = file_list.get('files', [])
    if not items:
        print('No files found in the folder.')
    else:
        print('Convert Excel to google sheet:')
        for item in items:
            try:
                file_metadata = {
                    'mimeType': 'application/vnd.google-apps.spreadsheet',
                    'parents': [cleaned_data_folder_id] # This is the crucial part
                }

                # Create a copy of the file with the new metadata
                copied_file = service.files().copy(
                    fileId=item['id'],
                    body=file_metadata,
                    fields='id, name, webViewLink' # Request specific fields to get back
                ).execute()
                
                new_file_id = copied_file.get('id')
                print(f"Successfully copied file as '{copied_file.get('name')}'")
                print(f"New File ID: {new_file_id}")
                
                
            except HttpError as error:
                print(f"An error occurred: {error}")
                print("Please check the file and folder IDs and ensure the service account has permission.")
                
    return items

def rename_file(creds, cleaned_data_folder_id:str):
    def list_files_in_folder(creds, folder_id, service:Resource):
        
        try:
          query = f"'{folder_id}' in parents"
          
          results = (
              service.files()
              .list(q=query, fields="nextPageToken, files(id, name, mimeType)")
              .execute()
          )
          items = results.get("files", [])
          return items

        except HttpError as error:
          print(f"An error occurred while listing files: {error}")
          return None
        
    drive_service = build("drive", "v3", credentials=creds)
    
    file_items = list_files_in_folder(creds, cleaned_data_folder_id, drive_service)
    client = gspread.authorize(creds)
    for file in file_items:
        if file['mimeType'] == "application/vnd.google-apps.spreadsheet" and "cleaned" not in file["name"]:
            get_sps = client.open_by_key(file["id"])
            sps_worksheet = get_sps.worksheet("main")
            indicator_value = sps_worksheet.acell("B2").value
            formatted_file_name = f"{indicator_value}_cleaned"
            body = {
              'name': formatted_file_name
            }
            updated_file = drive_service.files().update(
              fileId=get_sps.id,
              body=body,
              fields='id, name, webViewLink'
            ).execute()         
            print(f"File has been successfully renamed to {updated_file.get('name')}")

def update_metadata_cleaned_data(creds, cleaned_data_folder_id:str, metadata_file_id:str):
    def list_files_in_folder(folder_id, service:Resource):
        
        try:
          query = f"'{folder_id}' in parents"

          results = (
              service.files()
              .list(q=query, fields="nextPageToken, files(id, name, mimeType)")
              .execute()
          )
          items = results.get("files", [])
          return items

        except HttpError as error:
          print(f"An error occurred while listing files: {error}")
          return None
      
    gdrive_service = build("drive", "v3", credentials=creds)
    file_items = list_files_in_folder(cleaned_data_folder_id, gdrive_service)
    file_dict:dict = dict()
    sheets_service = build('sheets', 'v4', credentials=creds)
    print(f"Found {len(file_items)} files in the folder.")
    for file in file_items:
        if file['mimeType'] == "application/vnd.google-apps.spreadsheet" and file['name'].endswith("cleaned"):
            print(f"Processing file: {file['name']}")
            print(f"File ID: {file['id']}\n")
            sheet_id:str = file['id']
            sheet_name:str = file['name']
            try:
              sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            except HttpError:
              print(f"Quota Exceeded | wait a minute . . .")
              time.sleep(60)
              sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
            get_sheets = sheet_metadata.get('sheets', '')
            get_main_sheet = next((worksheet.get('properties', {}) for worksheet in get_sheets if worksheet['properties']['title'] == 'main') , None)
            if (get_main_sheet == None):
                raise Exception(f"Can't find Worksheet named 'main' from {sheet_name}")
            worksheet_id = get_main_sheet.get('sheetId')
            worksheet_title = get_main_sheet.get('title')
            file_dict[sheet_id] = {"sheet_name":sheet_name,
                                  "worksheet_id":worksheet_id,
                                  "worksheet_title":worksheet_title}
    print(f"Found {len(file_dict)} cleaned data files.")
    print("Updating metadata . . .")
    json_metadata = json.dumps(file_dict)
    bytes_file = io.BytesIO(json_metadata.encode("utf-8"))
    gdrive_media = MediaIoBaseUpload(bytes_file, mimetype="text/plain")
    updated_file = gdrive_service.files().update(
    fileId=metadata_file_id,
    media_body=gdrive_media).execute()
    print(f"File '{updated_file.get('name')}' was updated.")
    
    return file_dict
            
if __name__ == "__main__":
    
    CREDENTIALS_PATH = "" # Path to your credentials.json file
    SCOPES = ["https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/spreadsheets"]
    TOKEN_FILE_GDRIVE_ID = "" # The ID of the token file in Google Drive
    METADATA_CLEANED_FILE_ID = "" # The ID of the metadata file for cleaned data in Google Drive
       
    print("EXECUTE HELPER . . .")
    print("1.\tUPDATE TOKEN")
    print("2.\tCOPY EXCEL AS SPREADSHEET")
    print("3.\tRENAME ALL CLEANED TO FORMATTED")
    print("4.\tUPDATE METADATA OF CLEANED DATA")
    
    c = input("CHOOSE ONE . . . ")
    
    if c == "1":
        update(CREDENTIALS_PATH, SCOPES, TOKEN_FILE_GDRIVE_ID)
    elif c == "2":
        creds = load_credentials_from_gdrive(TOKEN_FILE_GDRIVE_ID, SCOPES)
        excel_handler_service = build('drive', 'v3', credentials=creds)
        excel_items = copy_excel_file(excel_handler_service,
                cleaned_data_folder_id= "1Jt5kwp5udHaMhm0N7VIWX6WrQpjPNFjr")
    elif c == "3":
        creds = load_credentials_from_gdrive(TOKEN_FILE_GDRIVE_ID, SCOPES)
        rename_file(creds, cleaned_data_folder_id="1Jt5kwp5udHaMhm0N7VIWX6WrQpjPNFjr")
    elif c == "4":
        print("Updating metadata of cleaned data . . .")
        print(update_metadata_cleaned_data(creds=load_credentials_from_gdrive(TOKEN_FILE_GDRIVE_ID, SCOPES),
                cleaned_data_folder_id="1Jt5kwp5udHaMhm0N7VIWX6WrQpjPNFjr",
                metadata_file_id=METADATA_CLEANED_FILE_ID))
    else:
        raise Exception("Invalid Key")
    pass