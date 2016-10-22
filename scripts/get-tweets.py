# coding=utf-8
from __future__ import print_function
import httplib2
import os
import time

from twitter import *
from config import Config

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'config/drive_client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def main():
    """Shows basic usage of the Sheets API.

    Creates a Sheets API service object and prints the names and majors of
    students in a sample spreadsheet:
    https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = '1FRbndGBB6xP-IzsJFSiCu9m7eLeyx1YfbUiBCnc8gBI'
    rangeName = 'Sheet1'

    # Traer configuración de Twitter API
    f = file('config/config.cfg')
    cfg = Config(f)

    twitter = Twitter(auth=OAuth(cfg.access_token_key,
                                 cfg.access_token_secret,
                                 cfg.consumer_key,
                                 cfg.consumer_secret))

    hashtags = ["#NiUnaMenos", "#VivasNosQueremos"]

    for hashtag in hashtags:

        max_id = None
        page_num = 0

        while page_num < 10000:
            values = []
            results = twitter.search.tweets(
                q=hashtag, count=100, include_entities=True, until="2016-10-20", since="2016-10-19", max_id=max_id)

            search_metadata = results['search_metadata']

            statuses = results['statuses']
            for status in statuses:
                if status['coordinates']:
                    timestamp = status['created_at']
                    lat = status['coordinates']['coordinates'][1]
                    lon = status['coordinates']['coordinates'][0]
                    termino = hashtag
                    tweet = status['text']
                    user = status['user']['screen_name']

                    # Crear row
                    value = [timestamp, lat, lon, termino, tweet, user]
                    print(value)
                    values.append(value)

            body = {'values': values}

            try:
                result = service.spreadsheets().values().append(spreadsheetId=spreadsheetId,
                                                                insertDataOption='INSERT_ROWS', range=rangeName, valueInputOption='RAW', body=body).execute()
            except Exception as e:
                print(e)

            try:
                max_id = search_metadata['next_results'].split(
                    "&")[0].split("max_id=")[1]
                print(max_id)
            except:
                break

            time.sleep(5)


if __name__ == '__main__':
    main()
