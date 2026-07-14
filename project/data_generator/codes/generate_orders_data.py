from faker import Faker
from datetime import datetime, timedelta
import random
from db.connection import MySqlConnection
import os 
import dotenv
dotenv.load_dotenv('/app/env/.env')

def generate_orders(min_orders,max_orders,from_date,to_date,statuses,payment_methods):
    min_orders = int(min_orders)
    max_orders = int(max_orders)
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.strptime(to_date,'%Y-%m-%d')
    
    if max_orders != 0:
        try:
            mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
            conn, cursor = mysql_connect.get_conn_n_cur()
            fake = Faker()
            cursor.execute(f''' USE {os.getenv('db')} ; ''') # type: ignore
        except Exception as e:
            print(f'Issue in server side as {e}')
        
        while from_date <= to_date:
            # Fetch customer registration dates
            cursor.execute(f"SELECT customer_id FROM customers WHERE created_on <= '{from_date}' ;") # type: ignore
            customers = cursor.fetchall()  # type: ignore 
            customer_with_valid_dates = [customer[0] for customer in customers]  # type: ignore

            num_orders = random.randint(min_orders,max_orders)
            
            for _ in range(num_orders):
                customer_id = random.choice(customer_with_valid_dates)
                order_date = from_date
                order_status = random.choices(statuses,weights=[5,20,25,40,5,5],k=1)[0]
                payment_method = random.choices(payment_methods,weights=[50,20,10,5,10,5],k=1)[0]
                shipping_address = fake.address()
                total_amount = 0
                discount_applied = 0 
                final_amount = 0

                query = f'''
                        INSERT INTO orders (customer_id, order_date,order_status,payment_method,shipping_address,total_amount,discount_applied,final_amount)
                        VALUES ({customer_id},'{order_date}','{order_status}','{payment_method}','{shipping_address}',{total_amount},{discount_applied},{final_amount}) ;
                '''
                
                cursor.execute(query) # type: ignore
            conn.commit() # type: ignore
            from_date = from_date + timedelta(1) # type: ignore
        
        print(f'Data written in orders table from {from_date} to {to_date}')
        mysql_connect.close_connections()
        print('All connection closed for orders data')
    else:
        print('No orders added')
            



def main():
    statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
    payment_methods = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'COD', 'Wallet']
    min_orders = os.getenv('min_orders')
    max_orders = os.getenv('max_orders')
    order_from_date = os.getenv('order_from_date')
    order_to_date = os.getenv('order_to_date')
    generate_orders(min_orders,max_orders,order_from_date,order_to_date,statuses,payment_methods)
    

if __name__ == "__main__":
    main()


# CREATE TABLE orders(
# 	oid INT AUTO_INCREMENT PRIMARY KEY,
#     customer_id INT,
#     order_date DATE,
#     order_status VARCHAR(15),
#     payment_method VARCHAR(15),
#     shipping_address TEXT,
#     total_amount float,
#     discount_applied float,
#     final_amount float
# );