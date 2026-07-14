import os 
import dotenv
import math
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def order_data(limit,page,from_date,to_date):
    try:
        ''' Establishing MySql connection  '''
        
        mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn,cursor = mysqlconnection.get_conn_n_cur()
        
    except Exception as e:
        print(f'MySQL connection problem :- {e} ')
    
    try:
        ''' Check for orders table in the db '''
        
        db_query = ''' USE demo; '''
        cursor.execute(db_query) # type:ignore
        
        table_query = ''' SELECT 1 FROM orders LIMIT 1;  '''
        cursor.execute(table_query) # type: ignore
        cursor.fetchall()  # type: ignore
        
    except Exception as e:
        print(f'Issue occured on db or table side as : {e}')
    
    cols = ["oid","customer_id","order_date","order_status","payment_method","shipping_address","total_amount","discount_applied","final_amount"]
    
    if from_date is None and to_date is None:
        count_records = ''' SELECT COUNT(oid) FROM orders; '''
        cursor.execute(count_records) # type: ignore
        total_records = cursor.fetchall()[0][0] # type: ignore
        if total_records == 0:
            total_pages = 0
            data = []
        else:
            
            count_records = f''' SELECT COUNT(oid) FROM orders ; '''
            cursor.execute(count_records) # type: ignore
            total_records = cursor.fetchall()[0][0] # type: ignore
            total_pages = math.ceil(total_records / limit)
            
            
            fetch_data = f''' SELECT oid,customer_id,order_date,order_status,payment_method,
                        shipping_address,total_amount,discount_applied,final_amount
                            FROM orders LIMIT {limit} OFFSET {(page - 1)*limit}; '''
            
            cursor.execute(fetch_data) # type:ignore
            query_result = cursor.fetchall() # type: ignore
            
            data = [ dict(zip(cols,row)) for row in query_result]
    else:
        if from_date is None and to_date is not None:
            
            count_records = f''' SELECT COALESCE(COUNT(oid),0) FROM orders WHERE order_date <= CAST('{to_date}' AS DATE) ; '''
            cursor.execute(count_records) # type: ignore
            total_records = cursor.fetchall()[0][0] # type: ignore
            
            total_pages = math.ceil(total_records / limit)
            
            result_query = f'''
            SELECT oid,customer_id,order_date,order_status,payment_method,
                shipping_address,total_amount,discount_applied,final_amount
                FROM orders WHERE order_date <= CAST('{to_date}' AS DATE) LIMIT {limit} OFFSET {(page - 1)*limit};
            '''
            cursor.execute(result_query) # type:ignore
            query_result = cursor.fetchall() # type: ignore
            
            data = [ dict(zip(cols,row)) for row in query_result]
            
        elif to_date is None and from_date is not None:
            count_records = f''' SELECT COALESCE(COUNT(oid),0) FROM orders WHERE CAST('{from_date}' AS DATE) <= order_date ; '''
            cursor.execute(count_records) # type: ignore
            total_records = cursor.fetchall()[0][0] # type: ignore
            
            total_pages = math.ceil(total_records / limit)
            
            result_query = f'''
            SELECT oid,customer_id,order_date,order_status,payment_method,
                shipping_address,total_amount,discount_applied,final_amount
                FROM orders WHERE CAST('{from_date}' AS DATE) <= order_date LIMIT {limit} OFFSET {(page - 1)*limit};
            '''
            cursor.execute(result_query) # type:ignore
            query_result = cursor.fetchall() # type: ignore
            
            data = [ dict(zip(cols,row)) for row in query_result]
        
        elif from_date > to_date:
            total_pages = 0
            data = []
        
        else:
            count_records = f''' SELECT COUNT(oid) FROM orders WHERE CAST(order_date AS DATE) BETWEEN CAST('{from_date}' AS DATE) AND CAST('{to_date}' AS DATE) ;'''
            cursor.execute(count_records) # type: ignore
            total_records = cursor.fetchall()[0][0] # type: ignore
            
            total_pages = math.ceil(total_records / limit)
            
            result_query = f'''
            SELECT oid,customer_id,order_date,order_status,payment_method,
                shipping_address,total_amount,discount_applied,final_amount
                FROM orders WHERE CAST(order_date AS DATE) BETWEEN CAST('{from_date}' AS DATE) AND CAST('{to_date}' AS DATE)
                LIMIT {limit} OFFSET {(page - 1)*limit};
            '''
            cursor.execute(result_query) # type:ignore
            query_result = cursor.fetchall() # type: ignore
            
            data = [ dict(zip(cols,row)) for row in query_result]
        
    mysqlconnection.close_connections()   
    
    return {"page_number": page,
        "limit":limit,
        "total_pages":total_pages,
        "data":data}

