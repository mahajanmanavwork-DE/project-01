from datetime import datetime
from codes.generate_customer_data import generate_customers
from codes.generate_product_data import generate_products
from codes.generate_orders_data import generate_orders
from codes.generate_order_items import generate_order_items
from codes.generate_inventory import generate_inventory
from codes.generate_order_logs import generate_order_status_log
import dotenv
import os

dotenv.load_dotenv('/app/env/.env')

country_with_cities = {
    "United States": [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", 
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte", 
        "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington D.C."
    ],
    
    "India": [
        "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", 
        "Chennai", "Kolkata", "Surat", "Pune", "Jaipur", 
        "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", 
        "Bhopal", "Visakhapatnam", "Patna", "Vadodara", "Agra"
    ],
    
    "China": [
        "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Tianjin", 
        "Chongqing", "Chengdu", "Wuhan", "Nanjing", "Hangzhou", 
        "Xian", "Suzhou", "Dongguan", "Foshan", "Shenyang", 
        "Qingdao", "Dalian", "Zhengzhou", "Changsha", "Kunming"
    ],
    
    "Brazil": [
        "São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza", 
        "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre", 
        "Goiânia", "Belém", "Guarulhos", "Campinas", "São Luís", 
        "Natal", "Teresina", "Campo Grande", "João Pessoa", "Maceió"
    ],
    
    "United Kingdom": [
        "London", "Manchester", "Birmingham", "Leeds", "Glasgow", 
        "Sheffield", "Bradford", "Liverpool", "Edinburgh", "Bristol", 
        "Cardiff", "Belfast", "Leicester", "Nottingham", "Newcastle", 
        "Southampton", "Aberdeen", "Norwich", "Coventry", "Plymouth"
    ],
    
    "Australia": [
        "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", 
        "Gold Coast", "Canberra", "Newcastle", "Wollongong", "Hobart", 
        "Darwin", "Townsville", "Cairns", "Geelong", "Toowoomba", 
        "Ballarat", "Bendigo", "Albury", "Launceston", "Mackay"
    ],
    
    "Japan": [
        "Tokyo", "Yokohama", "Osaka", "Nagoya", "Sapporo", 
        "Fukuoka", "Kobe", "Kyoto", "Kawasaki", "Saitama", 
        "Hiroshima", "Sendai", "Chiba", "Kitakyushu", "Sakai", 
        "Hamamatsu", "Niigata", "Okayama", "Kumamoto", "Nagasaki"
    ],
    
    "Germany": [
        "Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", 
        "Stuttgart", "Düsseldorf", "Leipzig", "Dortmund", "Essen", 
        "Bremen", "Dresden", "Hanover", "Nuremberg", "Duisburg", 
        "Bochum", "Wuppertal", "Bielefeld", "Bonn", "Mannheim"
    ],
    
    "Mexico": [
        "Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana", 
        "León", "Ciudad Juárez", "Zapopan", "Nezahualcóyotl", "Mérida", 
        "San Luis Potosí", "Aguascalientes", "Hermosillo", "Saltillo", "Mexicali", 
        "Culiacán", "Querétaro", "Morelia", "Chihuahua", "Toluca"
    ],
    
    "South Africa": [
        "Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth", 
        "Bloemfontein", "East London", "Pietermaritzburg", "Nelspruit", "Kimberley", 
        "Polokwane", "Rustenburg", "George", "Mbombela", "Bhisho", 
        "Mahikeng", "Mmabatho", "Upington", "Mthatha", "Ladysmith"
    ]
}

try:
    min_limit = os.getenv('min_limit')
    max_limit = os.getenv('max_limit')
    customers_from_date = os.getenv('customers_from_date')
    customers_to_date = os.getenv('customers_to_date')
    generate_customers(min_limit,max_limit,country_with_cities,customers_from_date,customers_to_date)
except Exception as e:
    print(f'Exception in generating customer details as {e}')
    

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

try:
    no_of_products = os.getenv('no_of_products')
    from_date = os.getenv('product_from_date')
    to_date = os.getenv('product_to_date')
    generate_products(int(no_of_products),categories = categories,brands = brands,from_date=from_date,to_date=to_date) # type: ignore
except Exception as e:
    print(f'Exception in generating products as {e}')

try:
    min_orders = os.getenv('min_orders')
    max_orders = os.getenv('max_orders')
    from_date = os.getenv('order_from_date')
    to_date = os.getenv('order_to_date')
    statuses = ['pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned']
    payment_methods = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'COD', 'Wallet']
    generate_orders(min_orders,max_orders,from_date,to_date,statuses,payment_methods)
except Exception as e:
    print(f'Exception in generating orders as {e}')
    
    
try:
    generate_order_items()
except Exception as e:
    print(f'Exception in generating order items as {e}')

try:
    generate_order_status_log()
except Exception as e:
    print(f'Exception in generating order status logs as {e}')

try:
    no_of_inventory_records = os.getenv('no_of_inventory_records')
    generate_inventory(int(no_of_inventory_records)) # type: ignore
except Exception as e:
    print(f'Exception in generating inventory as {e}')

