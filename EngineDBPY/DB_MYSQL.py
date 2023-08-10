import mysql.connector
from mysql.connector import Error

class DB_MYSQL:
    def __init__(self,server,db,admin,pswd):
        self.server = server
        self.db = db
        self.admin = admin 
        self.pswd = pswd
        self.cnxn = None
        self.cursor = None
        
    def Connect_db(self):
        try:
            self.cnxn = mysql.connector.connect(
                host = self.server,
                user = self.admin,
                passwd = self.pswd,
                database = self.db)
            db_info = self.cnxn.get_server_info()
            print("Conexion a MYSQL Server version:", db_info)
            self.cursor = self.cnxn.cursor()
        except Error as e:
            print("ERROR DE CONEXION:",e)
            
    def Close_db(self):
        self.cursor.close()
        self.cnxn.close()
        print("CONEXION FINALIZADA")
        
    def GET_ONE_ROW_db(self,sql_query):
        self.cursor.execute(sql_query)
        row = self.cursor.fetchone()
        return row[0]
        
    def GET_ROWS_db(self,sql_query):
        self.cursor.execute(sql_query)
        rows = self.cursor.fetchall()
        return rows
        
    def STORAGE_ROWS_db(self,sql_create,columns,rows,table_name):
        ### GENERACION DE LA TABLA DONDE SE ALMACENARA LA DATA
        self.cursor.execute(sql_create)
        self.cnxn.commit()
        
        num_columns = len(columns)
        ### INSERTAMOS LOS DATOS
        camp_names = '('
        for i in range(num_columns):
            camp_names = camp_names+columns[i] + ','
        camp_names = camp_names.strip(',')
        camp_names = camp_names+')'
        
        for row in rows:
            lrow = []
            for i in range(num_columns):
                if str(type(row[i])) == "<class 'datetime.datetime'>":
                    lrow.append(str(row[i])[:23])
                else:
                    lrow.append(str(row[i]))
            #CONSTRUYUENDO LA QUERY !!!  
            values = '('
            for elem in lrow:
                if elem == 'None':
                    values = values+'NULL'+','
                else:
                    values = values + f'{repr(elem)}'+','
            values = values.strip(',')
            values = values + ')'
                
            sql_insert = (
            fr'INSERT INTO {table_name} '
            fr'{camp_names} '
            r'values '
            fr'{values}'
                )
            try:
                self.cursor.execute(sql_insert)
                self.cnxn.commit()
            except:
                print("No se puedo subir el registro")
