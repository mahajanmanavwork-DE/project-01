import mysql.connector 

class MySqlConnection:
    connection = None 
    cusrsor = None
    def __init__(self,host,user,password):
        self.host = host
        self.user = user
        self.__password = password
        MySqlConnection.connection = self.__make_connection()
        MySqlConnection.cusrsor = MySqlConnection.connection.cursor()
        
    def __make_connection(self):
        connection = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = self.__password   
        )
        return connection
    
    def get_conn_n_cur(self):
        return MySqlConnection.connection, MySqlConnection.cusrsor 

    def close_connections(self):
        MySqlConnection.cusrsor.close() # type: ignore
        MySqlConnection.connection.close() # type: ignore