from faker import Faker
from datetime import datetime,timedelta
import random
import os 
import dotenv
from db.connection import MySqlConnection

dotenv.load_dotenv(r'/app/env/.env')


def generate_customers(min_limit,max_limit,country_with_cities : dict, from_date , to_date):
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.strptime(to_date, "%Y-%m-%d")
    print(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
    mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
    conn,cursor = mysql_connect.get_conn_n_cur()
    fake = Faker()
    country_names = list(country_with_cities.keys())
    weight = [10,30,10,10,10,4,5,5,3,2]
    cursor.execute(f"USE {os.getenv('db')};") # type: ignore
    if from_date > to_date:
        print('Invalid date arrangement')
    else:
        while (from_date <= to_date):
            num_records = random.randint(int(min_limit),int(max_limit))
            for _ in range(num_records):
                customer_name = fake.name()
                customer_email = fake.email()
                customer_country = random.choices(country_names,weight,k=1)[0]
                city_choice = country_with_cities[customer_country]
                customer_city = random.choice(city_choice)
                query = f''' INSERT INTO customers (name,email,city,country,created_on) 
                VALUES ('{customer_name}','{customer_email}','{customer_city}','{customer_country}','{from_date}') ;
                '''
                cursor.execute(query) #type: ignore
                conn.commit() # type: ignore
            from_date = from_date + timedelta(1)
        print(f'data written success for customer table')
        mysql_connect.close_connections()
        print('All connections closed ')

                
# CREATE TABLE customers(
# customer_id INT AUTO INCREMENT PRIMARY KEY,
# name VARCHAR(50) Not Null,
# email VARCHAR(50),
# city VARCHAR(50),
# country VARCHAR(50),
# created_on DATE
# );

