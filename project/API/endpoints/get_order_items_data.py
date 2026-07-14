import os 
import dotenv
import math
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def order_items_data(limit,page_no, on_date):
    try:
        mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn,cursor = mysqlconnection.get_conn_n_cur()
        cursor.execute('USE demo;') # type: ignore
        cursor.execute('SELECT 1 FROM order_items LIMIT 1;') # type: ignore
        cursor.fetchall() # type: ignore
    except Exception as e:
        print(f'Error occured in db as : {e}')
    else:
        cols = ["order_item_id", "order_id", "product_id", "quantity","unit_price_at_order", "discount_at_order", "line_total"]
        
        if on_date is None:
            
            data_count_query = ''' SELECT COUNT(order_id) FROM order_items; '''
        
            cursor.execute(data_count_query) # type: ignore
            data_count = cursor.fetchall()[0][0] # type: ignore
            
            if data_count > 0: # type: ignore
            
                fetch_query = f''' SELECT order_item_id, order_id, product_id, quantity, 
                                unit_price_at_order, discount_at_order, line_total
                                FROM order_items 
                                LIMIT {limit} OFFSET {(page_no - 1) * limit}; '''
                cursor.execute(fetch_query) # type: ignore
                result = cursor.fetchall() # type: ignore
                data = [dict(zip(cols,row)) for row in result]
            else:
                data = []
        
        else:
            
            data_count_query = f''' SELECT COUNT(oi.order_id) FROM order_items oi 
                                LEFT JOIN orders o ON oi.order_id = o.oid
                                WHERE o.order_date = '{on_date}'  ;  '''
        
            cursor.execute(data_count_query) # type: ignore
            data_count = cursor.fetchall()[0][0] # type: ignore
            
            if data_count > 0: # type: ignore
            
                fetch_query = f''' SELECT order_item_id, order_id, product_id, quantity, 
                                unit_price_at_order, discount_at_order, line_total
                                FROM order_items oi
                                LEFT JOIN orders o 
                                ON oi.order_id = o.oid
                                WHERE o.order_date = '{on_date}'
                                LIMIT {limit} OFFSET {(page_no - 1) * limit}; '''
                cursor.execute(fetch_query) # type: ignore
                result = cursor.fetchall() # type: ignore
                data = [dict(zip(cols,row)) for row in result]
            else:
                data = []
        
        mysqlconnection.close_connections()   
        
        return {
            "page_no" : page_no,
            "limit" : limit,
            "total_pages" : math.ceil(data_count/limit),
            "data" : data
        }