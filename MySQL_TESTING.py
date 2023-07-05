from EngineDBPY.DB_MYSQL import DB_MYSQL
import pandas as pd
server = '10.1.12.222'
admin = 'bihelpdesk'
pswd = 'rb1hd2023'
db =  'glpi-empty'
obj_database = DB_MYSQL(server,db,admin,pswd)
obj_database.Connect_db()

#Obteniendo la consulta para tickets
#%%
file = open('SQL_GET_TICKETS.txt','r')
sql_query = file.read()
file.close()
#%%

#Obteniendo los rows 
rows = obj_database.GET_ROWS_db(sql_query)
columns = ['TICKET_ID','TITULO','FECHA_CREACION','FECHA_RESOLUCION','PERSONAL_ASISTIDO','PERSONAL_ASIGNADO',
           'NIVEL_URGENCIA','NIVEL_IMPACTO','NIVEL_PRIORIDAD','CATEGORIA']
df = pd.DataFrame(columns = columns)
num_columns = len(columns)
for row in rows:
    lrow = []
    for i in range(num_columns):
        lrow.append(str(row[i]))
    print("AGREGANDO FILA:",df.shape[0])
    df.loc[df.shape[0]] = lrow    
        
obj_database.Close_db()

#%%

### ALMACENANDO A SQL SERVER
from EngineDBPY.DB_SQL_SERVER import DB_SQL_Server
#%%
server = 'AF06-PC-340096'
db = 'PRUEBAS'
admin = 'sa'
pswd = '123456SQLserver'
obj_database_sqlserver = DB_SQL_Server(server,db,admin,pswd)
#%%
obj_database_sqlserver.Connect_db()
sql_create = '''
DROP TABLE IF EXISTS Tickets
CREATE TABLE Tickets(
	ticket_id INT PRIMARY KEY,
	titulo varchar(250),
	fechacreacion DATETIME,
	fecharesolucion DATETIME,
	personalasistido varchar(250),
	personalasignado varchar(250),
	urgencia INT,
	impacto INT,
	prioridad INT,
	categoria VARCHAR(150)
)
'''
columns = ['ticket_id','titulo','fechacreacion','fecharesolucion',
           'personalasistido','personalasignado','urgencia','impacto','prioridad','categoria']
table_name = 'Tickets'

obj_database_sqlserver.STORAGE_ROWS_db(sql_create,columns,df.values,table_name)
obj_database_sqlserver.Close_db()

#%%

print(len(df.iloc[32,4]))
    
    




















