import json
import requests 
import s3fs
import os 
import dotenv
import sys
import datetime 
dotenv.load_dotenv(r'/opt/spark/env/.env')

def extract_raw_data(date):
    date = datetime.datetime.strptime(date,"%Y-%m-%d")
    base_url = "http://source_api_container:5000/"
    endpoints = ["get_user_data","get_products","get_orders","get_order_items","get_order_status_log","get_inventory"]
    fs = s3fs.S3FileSystem(endpoint_url= 'http://minio:9000',key=os.getenv('MINIO_ROOT_USER'), secret=os.getenv('MINIO_ROOT_PASSWORD') )
    
    for endpoint in endpoints:
        try:
            if endpoint == 'get_orders':
                main_url = base_url + endpoint + f'?from_date={date}&to_date={date}'
                total_pages = requests.get(main_url).json()['total_pages']
                if int(total_pages) != 0:
                    for page in range(1,total_pages + 1):
                        main_url = base_url + endpoint + f'?page_number={page}&from_date={date}&to_date={date}'
                        data = requests.get(main_url).json()["data"]
                        with fs.open(f's3://data//raw//{endpoint}//{str(date)[:10]}_file_{str(page)}.json','w') as file_object:
                            json.dump(data,file_object,indent=4)
                    print(f'Data inserted for {endpoint} endpoint')  
                else:
                    print(f'Endpoint {endpoint} has no data to insert')
            else:
                main_url = base_url + endpoint + f'?page_num=1&date={date}'
                total_pages = requests.get(main_url).json()['total_pages']
                if int(total_pages) != 0:
                    for page in range(1,total_pages + 1):
                        main_url = base_url + endpoint + f'?page_num={page}&date={date}'
                        data = requests.get(main_url).json()["data"]
                        with fs.open(f's3://data//raw//{endpoint}//{str(date)[:10]}_file_{str(page)}.json','w') as file_object:
                            json.dump(data,file_object,indent=4) 
                    print(f'Data inserted for {endpoint} endpoint')  
                else:
                    print(f'Endpoint {endpoint} has no data to insert')
        except Exception as e:
            print(f'Error is there as {e}')

if "--date" in sys.argv:
    date = sys.argv[sys.argv.index("--date") + 1]
    req_date = str(date)[:10]
    extract_raw_data(req_date)
else:
    print('No date given using 2026-07-01 as reference')
    req_date = "2026-07-01"
    extract_raw_data(req_date)
    

