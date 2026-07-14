import os 
import dotenv 
import math 
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def inventory_data(limit,page_no,date):
    try:
        mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn,cursor = mysqlconnection.get_conn_n_cur()
        db_query = ''' USE demo; '''
        cursor.execute(db_query) # type: ignore
        table_query = ''' SELECT 1 FROM inventory LIMIT 1 ;'''
        cursor.execute(table_query) # type: ignore
        cursor.fetchall() # type: ignore
    except Exception as e:
        print(r'Error in db or table side as {e}')
    else:
        cols = ["inventory_id","product_id","warehouse_location","quantity_on_hand","reorder_threshold","last_restocked_at"]
        
        if date is None:
            count_data_query = ''' SELECT COUNT(inventory_id) FROM inventory ;'''
            cursor.execute(count_data_query) # type: ignore
            count_data = cursor.fetchall()[0][0] # type: ignore
            
            fetch_query = f''' SELECT inventory_id,product_id,warehouse_location,quantity_on_hand,reorder_threshold,last_restocked_at
                            FROM inventory LIMIT {limit} OFFSET {(page_no - 1)*limit}; 
            '''
            cursor.execute(fetch_query) # type: ignore
            result = cursor.fetchall() # type: ignore
            
            data = [dict(zip(cols,row)) for row in result]
        
        else :
            count_data_query = f''' SELECT COUNT(inventory_id) FROM inventory WHERE last_restocked_at = CAST('{date}' as date)
            LIMIT {limit} OFFSET {(page_no - 1)*limit} ;'''
            cursor.execute(count_data_query) # type: ignore
            count_data = cursor.fetchall()[0][0] # type: ignore
            
            fetch_query = f''' SELECT inventory_id,product_id,warehouse_location,quantity_on_hand,reorder_threshold,last_restocked_at
                            FROM inventory WHERE last_restocked_at = CAST('{date}' as date) LIMIT {limit} OFFSET {(page_no - 1)*limit}; 
            '''
            cursor.execute(fetch_query) # type: ignore
            result = cursor.fetchall() # type: ignore
            
            data = [dict(zip(cols,row)) for row in result]
        # elif product_id is not None and date is None :
        #     count_data_query = f''' SELECT COUNT(inventory_id) FROM inventory WHERE product_id = {product_id} ;'''
        #     cursor.execute(count_data_query) # type: ignore
        #     count_data = cursor.fetchall()[0][0] # type: ignore
            
        #     fetch_query = f''' SELECT inventory_id,product_id,warehouse_location,quantity_on_hand,reorder_threshold,last_restocked_at,updated_at
        #                     FROM inventory WHERE product_id = {product_id} LIMIT {limit} OFFSET {(page_no - 1)*limit} ; 
        #     '''
        #     cursor.execute(fetch_query) # type: ignore
        #     result = cursor.fetchall() # type: ignore
            
        #     data = [dict(zip(cols,row)) for row in result]
        
        # else:
        #     count_data_query = f''' SELECT COUNT(inventory_id) FROM inventory WHERE product_id = {product_id} and  last_restocked_at = CAST('{date}' as date);'''
        #     cursor.execute(count_data_query) # type: ignore
        #     count_data = cursor.fetchall()[0][0] # type: ignore
            
        #     fetch_query = f''' SELECT inventory_id,product_id,warehouse_location,quantity_on_hand,reorder_threshold,last_restocked_at,updated_at
        #                     FROM inventory WHERE product_id = {product_id} and last_restocked_at = CAST('{date}' as date) LIMIT {limit} OFFSET {(page_no - 1)*limit}; 
        #     '''
        #     cursor.execute(fetch_query) # type: ignore
        #     result = cursor.fetchall() # type: ignore
            
        #     data = [dict(zip(cols,row)) for row in result]
    
    mysqlconnection.close_connections()        
    return {
        "page_number":page_no,
        "limit":limit,
        "total_pages" : math.ceil(count_data / limit),
        "data" : data
    }
        
            
            
        