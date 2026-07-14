import math
import os 
import dotenv
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def user_data(limit : int = 500,page_num : int = 1,request_date = None):
    mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
    conn,cursor = mysqlconnection.get_conn_n_cur()
    cursor.execute('USE demo;') # type: ignore
    
    if request_date is None:
        cursor.execute(f'SELECT COUNT(customer_id) FROM customers') # type: ignore
        total_records = cursor.fetchone()[0] # type: ignore
        total_pages = math.ceil(total_records / limit) # type: ignore
        cursor.execute(f'SELECT * from customers ORDER BY customer_id LIMIT {limit} OFFSET {(page_num - 1 )*limit};') # type: ignore
        results = cursor.fetchall() # type: ignore
        cols = ["customer_id","name","email","city","country","join_date"]
        data = [dict(zip(cols,row)) for row in results]
    else:
        cursor.execute(f"SELECT COUNT(customer_id) FROM customers WHERE created_on = '{request_date}'") # type: ignore
        total_records = cursor.fetchone()[0] # type: ignore
        
        if (total_records is not None) and (total_records > 0): # type: ignore
            total_pages = math.ceil(total_records / limit) # type: ignore
            cursor.execute(f"SELECT * from customers WHERE created_on = '{request_date}' LIMIT {limit} OFFSET {(page_num - 1 )*limit};") # type: ignore
            results = cursor.fetchall() # type: ignore
            cols = ["customer_id","name","email","city","country","join_date"]
            data = [dict(zip(cols,row)) for row in results]
        else:
            total_pages = 0
            total_records = 0
            data = []
            
            
    mysqlconnection.close_connections() 
    
    return {
        "page_number":page_num,
        "limit":limit,
        "total_pages":total_pages,
        "total_records":total_records,
        "data":data
    }