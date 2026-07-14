import os 
import dotenv
import math
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def product_data(limit,page,on_date):
    try:
        mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn,cursor = mysqlconnection.get_conn_n_cur()
        db_query = 'USE demo;'
        cursor.execute(db_query) # type: ignore
    
        # Checks for table and data 
    
        table_check = 'SELECT pid FROM products LIMIT 1'
        cursor.execute(table_check) # type: ignore
        cursor.fetchall() # type:ignore
        
    except Exception as e:
        print(f'Error in connections as {e}')
        return {
            "page_number": 0,
            "limit":limit,
            "total_pages": 0,
            "data": []
        }
    else:
        if on_date is None:
            total_records_query = f'''SELECT COUNT(pid) FROM products ;'''
            cursor.execute(total_records_query) # type: ignore
            total_records = cursor.fetchone()[0] # type: ignore
            total_pages = math.ceil(total_records / limit )
            table_query = f'''SELECT pid,product_name,category,sub_category,
            brand,base_price,discount_pct,current_stock,supplier,created_at
            FROM products 
            ORDER BY pid
            LIMIT {limit} OFFSET {(page - 1 )* limit} ;'''
            
                
        else:
            total_records_query = f'''SELECT COUNT(pid) FROM products WHERE created_at = '{on_date}' ;'''
            cursor.execute(total_records_query) # type: ignore
            total_records = cursor.fetchone()[0] # type: ignore
            total_pages = math.ceil(total_records / limit )
            table_query = f'''SELECT pid,product_name,category,sub_category,
            brand,base_price,discount_pct,current_stock,supplier,created_at
            FROM products 
            WHERE created_at = '{on_date}'
            ORDER BY pid
            LIMIT {limit} OFFSET {(page - 1 )* limit} ;'''
                
        cursor.execute(table_query) # type: ignore
        query_result = cursor.fetchall() # type: ignore
        cols =  ["pid","product_name","category","sub_category","brand","base_price","discount_pct","current_stock","supplier","created_at"]
        data = [dict(zip(cols,record)) for record in query_result]
                
        mysqlconnection.close_connections() 

    return {
        "page_number": page,
        "limit":limit,
        "total_pages":total_pages,
        "data":data
        }
    
        
        
    

