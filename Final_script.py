import pickle
import os
from pprint import pprint as pp
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import pandas as pd

def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())

        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open('token.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'Service created successfully')
        return(service)
    except Exception as e:
        print(e)
        return None

#defining gsheets api parameters
CLIENT_SECRET_FILE = 'credentials.json'
API_SERVICE_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
gsheetId = '1p0WapztVfHOtuDH_zW-PpjWNO21XoEBk5P1iu0i1wlE' #source gsheet
gsheetId_destination = '1y61nXG1QXLiREGevgcxdAk95fgniS2SsB2iPYDc43KM' #destination gsheet

#creating lists of sheets from gsheets
source_range = ['Source', 'Place', 'Time', 'Summary']
destination_range = ['Sheet2!A1', 'Sheet3!A1', 'Sheet4!A1', 'Sheet5!A1']
clear_range = ['Sheet2', 'Sheet3', 'Sheet4', 'Sheet5']

for i,x,j in zip(source_range, destination_range, clear_range):
    s = Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    gs = s.spreadsheets()

#importing gsheets data as dataframes
    rows = gs.values().get(spreadsheetId=gsheetId, range=i).execute()
    data = rows.get('values')
    df = pd.DataFrame(data)
    df.columns = df.iloc[0]
    print(df)

#clearing gsheets to avoid repition
    s.spreadsheets().values().clear(
        spreadsheetId = gsheetId_destination,
        range=j
    ).execute()

#exporting dataframe values to destination gsheet
    response_date = s.spreadsheets().values().append(
        spreadsheetId = gsheetId_destination,
        valueInputOption='RAW',
        range=x,
        body=dict(
            majorDimension='ROWS',
            values=df.values.tolist()
        )
    ).execute()