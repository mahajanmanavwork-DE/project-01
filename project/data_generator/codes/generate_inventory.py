from faker import Faker
from datetime import datetime,date
import random
from db.connection import MySqlConnection
import os 
import dotenv

dotenv.load_dotenv(r'/app/env/.env')

def generate_inventory(limit):
    limit = int(limit)
    if limit != 0:
        try:
            mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
            conn, cursor = mysql_connect.get_conn_n_cur()
            fake = Faker()
            cursor.execute(f"USE {os.getenv('db')} ;") # type: ignore
        except Exception as e:
            print(e)
        
        warehouses = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad']
        
        product_ids = {}
        cursor.execute('SELECT p.pid,COALESCE(i.last_restocked_at,p.created_at) as last_date FROM products p LEFT JOIN inventory i ON p.pid = i.product_id ORDER BY COALESCE(i.last_restocked_at,p.created_at) DESC ;') # type: ignore
        product_data = cursor.fetchall() # type: ignore
        for product in product_data:
            product_ids[product[0]] = product[1] # type: ignore
        
        for _ in range(limit):
            product_id = random.choice(list(product_ids.keys()))
            warehouse_location = random.choice(warehouses)
            quantity_on_hand = random.randint(5,100)
            reorder_threshold = random.randint(15,30)
            last_restock_date = fake.date_between_dates(product_ids[product_id],date.today())
        
            cursor.execute(f''' INSERT INTO inventory (product_id,warehouse_location,quantity_on_hand,reorder_threshold,last_restocked_at)
                        VALUES ({product_id},'{warehouse_location}',{quantity_on_hand},{reorder_threshold},'{last_restock_date}');
                        ''') 
            conn.commit() # type: ignore
        
        print('Data added successfully for inventory table')
        mysql_connect.close_connections()
        print('All connections closed for inventory table ')
    else:
        print('No inventory record added')
if __name__ == "__main__":
    
    generate_inventory(limit=20)

# CREATE TABLE inventory (
#     inventory_id INT PRIMARY KEY AUTO_INCREMENT,
#     product_id INT,
#     warehouse_location VARCHAR(50),
#     quantity_on_hand INT DEFAULT 0,
#     reorder_threshold INT DEFAULT 10,
#     last_restocked_at DATE,
#     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# );