from faker import Faker
from datetime import datetime
import random
from db.connection import MySqlConnection
import os 
import dotenv

dotenv.load_dotenv(r'/app/env/.env')

def generate_order_items():
    try:
        mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn, cursor = mysql_connect.get_conn_n_cur()
        cursor.execute(f" USE {os.getenv('db')}; ") # type: ignore
    
    except Exception as e:
        print(f'Issue on server side as {e}')
        
    else:
        cursor.execute(''' SELECT o.oid FROM orders o LEFT JOIN order_items oi ON o.oid = oi.order_id WHERE oi.order_id is NULL ; ''') # type: ignore
        orders = cursor.fetchall() # type: ignore
        order_ids = []  
        for order in orders:
            order_ids.append(order[0]) # type: ignore
        
        product_prices = {}
        cursor.execute(f''' SELECT DISTINCT pid,base_price,discount_pct FROM products; ''') # type: ignore
        data = cursor.fetchall() # type: ignore 
        for row in data:
            product_prices[row[0]] = {'base_price':row[1], 'discount_pct':row[2]} # type: ignore
                
        products = list(product_prices.keys())
        if len(products) != 0:      
            for order_id in order_ids:
                num_products = random.randint(1,7)
                for _ in range(num_products):
                    product_id = random.choice(products)
                    num_qty = random.randint(1,5)
                    unit_price = product_prices[product_id]['base_price']
                    discount_pct = product_prices[product_id]['discount_pct']
                    line_total = ((unit_price*num_qty) - (unit_price*num_qty)*(discount_pct/100))
                    
                    cursor.execute(f''' INSERT INTO order_items (order_id,product_id,quantity,unit_price_at_order,discount_at_order,line_total)
                                    VALUES ({order_id},{product_id},{num_qty},{unit_price},{discount_pct},{line_total});
                                ''') # type: ignore
                conn.commit() # type: ignore
            
            print('Data Insertion success for order items ')
            mysql_connect.close_connections()
            print('Connection closed success for order items ')
        else:
            print('No order items added')
                

if __name__ == "__main__":
    generate_order_items()

# CREATE TABLE order_items (
#     order_item_id INT PRIMARY KEY,
#     order_id INT,
#     product_id INT,
#     quantity INT,
#     unit_price_at_order DECIMAL(10,2),
#     discount_at_order DECIMAL(5,2) DEFAULT 0.00,
#     line_total DECIMAL(10,2)
# );