from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run_flow
from oauth2client.file import Storage
import json
import os
import re
import httplib2 
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
import requests
import datetime
import csv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


''' Function to check whether file exist in the path or not '''
def where_json(file_name):
    return os.path.exists(file_name)

''' Function to return the refresh token '''
def get_refresh_token(client_id, client_secret):
    CLIENT_ID = client_id
    CLIENT_SECRET = client_secret
    SCOPE = 'https://www.googleapis.com/auth/analytics.readonly'
    REDIRECT_URI = 'http:localhost:8080'
  
    flow = OAuth2WebServerFlow(client_id = CLIENT_ID, client_secret = CLIENT_SECRET, 
        scope = SCOPE, redirect_uri = REDIRECT_URI)
    if where_json('credential.json') == False:
       storage = Storage('credential.json') 
       credentials = run_flow(flow, storage)
       refresh_token = credentials.refresh_token
       
    elif where_json('credential.json') == True:
       with open('credential.json') as json_file:  
           data = json.load(json_file)
       refresh_token = data['refresh_token']
  
    return(refresh_token)

''' Function to return the google analytics data for given dimension, metrics, start data, 
    end data access token, type,goal number, condition'''
def google_analytics_reporting_api_data_extraction(viewID, dim, met, start_date, end_date, 
    refresh_token, transaction_type, goal_number, condition):
     
    viewID = viewID; dim = dim; met = met; start_date = start_date; end_date = end_date
    refresh_token = refresh_token; transaction_type = transaction_type; condition = condition
    goal_number = goal_number
    viewID = "".join(['ga%3A', viewID])
    
    if transaction_type == "Goal":
        met1 = "%2C".join([re.sub(":", "%3A", i) for i in met]).replace("XX", str(goal_number))
    elif transaction_type == "Transaction":
        met1 = "%2C".join([re.sub(":", "%3A", i) for i in met])
        
    dim1 = "%2C".join([re.sub(":", "%3A", i) for i in dim])
    
    if where_json('credential.json') == True:
        with open('credential.json') as json_file:  
            storage_data = json.load(json_file)
       
        client_id = storage_data['client_id']
        client_secret = storage_data['client_secret']
        credentials = client.OAuth2Credentials(
            access_token = None, client_id = client_id, client_secret = client_secret, 
            refresh_token = refresh_token, token_expiry = 3600, token_uri = GOOGLE_TOKEN_URI,
            user_agent = 'my-user-agent/1.0', revoke_uri = GOOGLE_REVOKE_URI)

        credentials.refresh(httplib2.Http())
        rt = (json.loads(credentials.to_json()))['access_token']
  
        api_url = "https://www.googleapis.com/analytics/v3/data/ga?ids="
    
        url = "".join([api_url, viewID, '&start-date=', start_date, '&end-date=', end_date, '&metrics=', met1,
            '&dimensions=', dim1, '&max-results=1000000', condition, '&access_token=', rt])
    
        try:
            response = requests.get(url)
            
            try:
                data = response.text
                parsed = json.loads(data)['rows']
                
                with open('/home/ashekcse/data/ga_data.csv', 'w') as outcsv:
                    writer = csv.writer(outcsv, delimiter = ',', quoting = csv.QUOTE_MINIMAL, lineterminator = '\n')
                    for item in parsed:
                        #Write item to outcsv
                        writer.writerow([item[0].encode("utf-8"), item[1].encode("utf-8"), item[2].encode("utf-8"),
                            item[3].encode("utf-8"), item[4].encode("utf-8"), item[5].encode("utf-8"), 
                            item[6].encode("utf-8"), item[7].encode("utf-8")])
                
                print("Data extraction is successfully completed")
                
                return parsed
            except:
                print((response.json()))
        except:
            print((response.json()))
            print("Error occured in the google analytics reporting api data extraction")

''' function creates BigQuery dataset if not exists '''
def bq_create_dataset(project_id, dataset_id):
    bigquery_client = bigquery.Client(project = project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)

    try:
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

''' function creates BigQuery table if not exists ''' 
def bq_create_table(project_id, dataset_id, table_name, schema):
    bigquery_client = bigquery.Client(project = project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        bigquery_client.get_table(table_ref)

    except NotFound:
        schema = schema
        table = bigquery.Table(table_ref, schema = schema)
        table = bigquery_client.create_table(table)
        print('Table {} created.'.format(table.table_id))

''' function loadcsv file data into BigQuery table '''
def bq_load_csv(project_id, dataset_id, table_name, schema):
    bigquery_client = bigquery.Client(project = project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.schema = schema
    #job_config.skip_leading_rows = 1
    
    with open('/home/ashekcse/data/ga_data.csv', "rb") as source_file:
        job = bigquery_client.load_table_from_file(source_file, table_ref, job_config = job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{} table.".format(job.output_rows, dataset_id, table_name))

''' Main function '''
if __name__ == "__main__" :
    client_id = '403612370386-49928n2j1smekrjgv62hs3eekgi1rgpq.apps.googleusercontent.com'
    client_secret = 'EknixQj6k7XkZgw8JKu-SUqA'
    refresh_token = get_refresh_token(client_id, client_secret)

    viewID = '147522790'
    #dim = ['ga:dimension1','ga:dimension3','ga:pagePath','ga:channelGrouping','ga:medium','ga:source','ga:referralPath','ga:fullReferrer']
    dim = ['ga:pagePath', 'ga:channelGrouping', 'ga:medium', 'ga:source', 'ga:referralPath', 'ga:fullReferrer']
    met = ['ga:sessions', 'ga:pageViews']
    start_date = '2019-10-01'
    end_date = datetime.date.today().strftime("%Y-%m-%d")
    transaction_type = 'Transaction'
    goal_number = ''
    refresh_token = refresh_token
    condition = ''
    project_id = 'test-assignment-14-ashekur'
    dataset_id = 'ga_analytics_dataset'
    table_name = 'ga_analytics_data_' + datetime.datetime.now().strftime("%Y%m%d%H%M")
    schema = [
        bigquery.SchemaField('pagePath', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('channelGrouping', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('referralPath', 'STRING'),
        bigquery.SchemaField('fullReferrer', 'STRING'),
        bigquery.SchemaField('sessions', 'INTEGER'),
        bigquery.SchemaField('pageViews', 'INTEGER'),
    ]

    data = google_analytics_reporting_api_data_extraction(
        viewID, dim, met, start_date, end_date, refresh_token, transaction_type, goal_number, condition)
    bq_create_dataset(project_id, dataset_id)
    bq_create_table(project_id, dataset_id, table_name, schema)
    bq_load_csv(project_id, dataset_id, table_name, schema)

