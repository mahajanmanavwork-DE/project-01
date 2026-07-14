from fastapi import FastAPI
from endpoints.get_user_data import user_data
from endpoints.get_product_data import product_data
from endpoints.get_order_data import order_data
from endpoints.get_order_items_data import order_items_data
from endpoints.get_order_status_log_data import order_status_log_data
from endpoints.get_inventory_data import inventory_data
from datetime import datetime,date

app = FastAPI()


''' This endpoint can return all the data from customers table and you can provide specific date for daily records '''
@app.get('/get_user_data')
def get_user_data(limit :int = 500 ,page_num :int = 1,date = None):
    return user_data(limit,page_num,date)

 
''' This endpoint can return all the data from products tables '''
@app.get('/get_products')
def get_product_data(limit : int = 500,page_num : int = 1,date = None):
    return product_data(limit,page_num,date)


''' This endpoint return all records from orders table, you can also apply date filters using from_date and to_date '''
@app.get('/get_orders')
def get_orders_data(limit : int = 1000, page_num : int = 1,from_date : date = None, to_date : date = None ): # type: ignore
    return order_data(limit,page_num,from_date,to_date)


''' This endpoint return all the records from order items table, if you want data from specific order_id use the parameter order_id '''
@app.get('/get_order_items')
def get_order_items_data(limit : int = 1000, page_num : int = 1, date : date = None): # type: ignore
    return order_items_data(limit,page_num,date)


''' This endpoint return all the records from order status log table, if you want data from specific order_id use the paramether order_id '''
@app.get('/get_order_status_log')
def get_order_status_log_data(limit : int = 1000, page_num : int = 1, date : date = None): # type: ignore
    return order_status_log_data(limit,page_num,date )


@app.get('/get_inventory')
def get_inventory_data(limit : int = 500 ,page_num : int = 1,date : date = None ): # type: ignore
    return inventory_data(limit,page_num,date)