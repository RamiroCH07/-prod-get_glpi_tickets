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
           'NIVEL_URGENCIA','NIVEL_IMPACTO','NIVEL_PRIORIDAD','CATEGORIA','COMENTARIO_CATEGORIA']
df = pd.DataFrame(columns = columns)
num_columns = len(columns)
for row in rows:
    lrow = []
    for i in range(num_columns):
        lrow.append(str(row[i]))
    print(lrow)
    df.loc[df.shape[0]] = lrow    
        
obj_database.Close_db()

#%%

