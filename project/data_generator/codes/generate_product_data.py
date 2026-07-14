from faker import Faker
from datetime import datetime
import random
from db.connection import MySqlConnection
import os
import dotenv

dotenv.load_dotenv(r'/app/env/.env')

def generate_products(limit : int ,categories,brands,from_date,to_date):
    from_date = datetime.strptime(from_date,"%Y-%m-%d")
    to_date = datetime.strptime(to_date,"%Y-%m-%d")
    try:
        mysql_connect = MySqlConnection(os.getenv('mysql_host'),os.getenv('mysql_user'),os.getenv('mysql_pass'))
        conn, cursor = mysql_connect.get_conn_n_cur()
        fake = Faker()
        cursor.execute(f''' USE {os.getenv('db')} ; ''')  # type: ignore
    except Exception as e:
        print(f'Server side error as {e}')
    else:
        limit = int(limit)
        if limit != 0:
            for pid in range(1, limit + 1):
                category = random.choice(list(categories.keys()))
                sub_category = random.choice(categories[category])
                brand = random.choice(brands)
                product_name = f"{brand} {sub_category} {fake.word().capitalize()} {'Pro' if random.random() > 0.5 else ''}"
                base_price = round(random.uniform(99, 99999), 2)
                discount_pct = round(random.uniform(0, 45), 2)
                current_stock = random.randint(0, 500)
                supplier = fake.company()
                created_at = fake.date_between_dates(from_date, to_date)
                
                query = f'''
                INSERT INTO products VALUES (
                    {pid+1000}, "{product_name}", "{category}", "{sub_category}", "{brand}",
                    {base_price}, {discount_pct}, {current_stock}, "{supplier}", "{created_at}"
                );
                '''
                try:
                    cursor.execute(query) # type: ignore
                except Exception as e:
                    print(f"Error at PID {pid}: {e}")
            
            conn.commit() # type: ignore
            
            print(f"{limit} Products Added")
            mysql_connect.close_connections()
            print('All connections closed for product table')
        else:
            print('No products added')

def main():
    
    categories = {
            "Electronics": ["Smartphones", "Laptops", "Headphones", "Tablets", "Cameras", "Smartwatches"],
            "Clothing": ["Men's Wear", "Women's Wear", "Kids Wear", "Sportswear", "Ethnic Wear", "Winter Wear"],
            "Home & Kitchen": ["Furniture", "Kitchen Appliances", "Home Decor", "Bedding", "Lighting", "Storage"],
            "Books": ["Fiction", "Non-Fiction", "Academic", "Children", "Comics", "Self-Help"],
            "Sports & Fitness": ["Equipment", "Footwear", "Apparel", "Accessories", "Nutrition", "Yoga"],
            "Beauty & Health": ["Skincare", "Haircare", "Makeup", "Fragrances", "Wellness", "Personal Care"],
            "Automotive": ["Car Accessories", "Bike Accessories", "Tools", "Oils & Fluids", "Electronics", "Tyres"],
            "Toys & Games": ["Action Figures", "Board Games", "Puzzles", "Educational", "Outdoor", "Soft Toys"]
        }

    brands = ["TechPro", "HomeEssentials", "StyleHub", "FitGear", "BookWorm", "GlamUp", 
                "AutoX", "PlayZone", "Nova", "PrimeSelect", "UrbanTrend", "EcoVibe"]

    from_date = os.getenv('product_from_date')
    to_date = os.getenv('product_to_date')
    no_of_products = int(os.getenv('no_of_products')) # type: ignore
    
    generate_products(no_of_products,categories,brands,from_date,to_date)

if __name__ == "__main__":
    main()



# CREATE TABLE products(
#     pid INT PRIMARY KEY,
#     product_name VARCHAR(100),
#     category VARCHAR(50),
#     sub_category VARCHAR(50),
#     brand VARCHAR(50),
#     base_price FLOAT,
#     discount_pct FLOAT,
#     current_stock INT,
#     supplier VARCHAR(100),
#     created_at DATE
# );

