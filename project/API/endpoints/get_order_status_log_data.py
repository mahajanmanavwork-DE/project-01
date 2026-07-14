import os 
import dotenv
import math 
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')

def order_status_log_data(limit,page_no,on_date):
    try:
        mysqlconnection = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn,cursor = mysqlconnection.get_conn_n_cur()
        db_query = ''' USE demo; '''
        cursor.execute(db_query) # type: ignore
        table_query = ''' SELECT 1 FROM order_status_log LIMIT 1 ;'''
        cursor.execute(table_query) # type: ignore
        cursor.fetchall() # type: ignore
    except Exception as e:
        print(r'Error in db or table side as {e}')
    else:
        cols = ["log_id", "order_id", "old_status", "new_status", "changed_at", "changed_by", "remarks"]
        if on_date is None:
            count_data_query = '''SELECT COUNT(log_id) FROM order_status_log ; '''
            cursor.execute(count_data_query) # type: ignore
            data_count = cursor.fetchall()[0][0] # type: ignore
            
            fetch_query = f''' SELECT log_id, order_id, old_status, new_status, changed_at, changed_by, remarks
                                FROM order_status_log LIMIT {limit} OFFSET {(page_no - 1) * limit} ; '''
            cursor.execute(fetch_query)  # type: ignore 
            result = cursor.fetchall()  # type: ignore
            data = [dict(zip(cols,row)) for row in result]
        else:
            count_data_query = f'''SELECT COUNT(osl.log_id) 
                                    FROM order_status_log osl 
                                    LEFT JOIN orders o 
                                    ON osl.order_id = o.oid
                                    WHERE o.order_date = '{on_date}';
                                '''
            cursor.execute(count_data_query) # type: ignore
            data_count = cursor.fetchall()[0][0] # type: ignore
            
            fetch_query = f''' SELECT osl.log_id, osl.order_id, osl.old_status, osl.new_status, osl.changed_at, osl.changed_by, osl.remarks
                                FROM order_status_log osl 
                                LEFT JOIN orders o 
                                ON osl.order_id = o.oid
                                WHERE o.order_date = '{on_date}'
                                LIMIT {limit} OFFSET {(page_no - 1) * limit}; '''
            cursor.execute(fetch_query) # type: ignore 
            result = cursor.fetchall() # type: ignore
            data = [dict(zip(cols,row)) for row in result]
    
    mysqlconnection.close_connections()   
         
    return {"page_no" : page_no,
            "limit" : limit,
            "total_pages" : math.ceil(data_count / limit),
            "data" : data
            }