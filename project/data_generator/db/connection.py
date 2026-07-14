import mysql.connector 
import time
class MySqlConnection:
    connection = None 
    cursor = None
    def __init__(self,host,user,password):
        self.host = host
        self.user = user
        self.__password = password
        MySqlConnection.connection = self.__make_connection()
        MySqlConnection.cursor = MySqlConnection.connection.cursor() # type: ignore
        
    def __make_connection(self):
        for i in range(10):
            try:
                connection = mysql.connector.connect(
                    host = self.host,
                    user = self.user,
                    password = self.__password   
                )
            except:
                print(f'Tried {i+1} time')
                time.sleep(5)
            else:
                return connection
                
    
    def get_conn_n_cur(self):
        return MySqlConnection.connection, MySqlConnection.cursor 

    def close_connections(self):
        MySqlConnection.cursor.close() # type: ignore
        MySqlConnection.connection.close() # type: ignore