from faker import Faker
from datetime import datetime, timedelta
import random
from db.connection import MySqlConnection
import os 
import dotenv

dotenv.load_dotenv(r'/app/env/.env')

def generate_order_status_log():
    mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
    conn, cursor = mysql_connect.get_conn_n_cur()
    cursor.execute(f"USE {os.getenv('db')} ;") # type: ignore
    
    # Fetch all orders using your column name: oid
    cursor.execute("SELECT oid, order_date, order_status FROM orders;") # type: ignore
    orders = cursor.fetchall() # type: ignore
    
    status_flow = {
        'pending': ['confirmed', 'cancelled'],
        'confirmed': ['shipped', 'cancelled'],
        'shipped': ['delivered', 'returned'],
        'delivered': ['returned'],
        'returned': [],
        'cancelled': []
    }
    
    changed_by_options = ['system', 'admin', 'customer']
    
    for oid, order_date, current_status in orders:
        order_dt = order_date
        
        # For date type, convert to datetime for timedelta
        if isinstance(order_dt, str):
            order_dt = datetime.strptime(str(order_dt), '%Y-%m-%d')
        
        while current_status in status_flow and status_flow[current_status]:
            if random.random() < 0.4:  # 40% chance to stop progression
                break
            
            new_status = random.choice(status_flow[current_status])
            hours_passed = random.randint(1, 72)
            changed_at = order_dt + timedelta(hours=hours_passed) # type: ignore
            
            query = f'''
            INSERT INTO order_status_log (order_id, old_status, new_status, changed_at, changed_by, remarks)
            VALUES ({oid}, "{current_status}", "{new_status}", "{changed_at}", "{random.choice(changed_by_options)}", "Status changed from {current_status} to {new_status}");
            '''

            cursor.execute(query) # type: ignore
            order_dt = changed_at  # Next change must be after this
            current_status = new_status
            
            query = f''' UPDATE orders SET order_status = {new_status} WHERE oid = {oid}; '''

        conn.commit() # type: ignore
        
    print('Data inserted for order logs ')
    mysql_connect.close_connections()
    print('All connections closed for order logs ')
        

if __name__ == "__main__":   
    generate_order_status_log()


# CREATE TABLE order_status_log (
#     log_id INT PRIMARY KEY AUTO_INCREMENT,
#     order_id INT,
#     old_status VARCHAR(20),
#     new_status VARCHAR(20),
#     changed_at DATETIME,
#     changed_by VARCHAR(20),
#     remarks VARCHAR(255)
# );