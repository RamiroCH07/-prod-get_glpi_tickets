from EngineDBPY.DB_MYSQL import DB_MYSQL
import pandas as pd
import numpy as np
#%%
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
columns = ['TICKET_ID','TITULO','DESCRIPCION','FECHA_CREACION','FECHA_RESOLUCION','PERSONAL_ASISTIDO','PERSONAL_ASIGNADO',
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


#%%% PREPROCESAMIENTO DE DATOS
# ELIMINANDO RUIDO DE LOS TEXTOS DEL CAMPO DESCRIPCION

def transform_content(txt):
    if txt.startswith('&lt;div') or txt.startswith('&lt;meta') or txt.startswith('&lt;table') or txt.startswith('<div'):
        txt = 'mail'
    else:
        txt_split = txt.split(';')
        if len(txt_split) > 1:
            txt = txt_split[2].strip('.&lt')
    return txt
        

df['DESCRIPCION'] = df['DESCRIPCION'].map(transform_content)


#%% COLOCAMOS EL TITULO POR LOS QUE DATOS QUE FUERON IDENTIFICADOS COMO MAILS
df['DESCRIPCION'] = np.where(
    df['DESCRIPCION'] == 'mail',
    df['TITULO'],
    df['DESCRIPCION']
    )
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

columns = ['ticket_id','titulo','descripcion','fechacreacion','fecharesolucion',
           'personalasistido','personalasignado','urgencia','impacto','prioridad','categoria']

table_name = 'Tickets'

obj_database_sqlserver.STORAGE_ROWS_db('',columns,
                                       df.values,
                                       table_name,
                                       ADD_NEW_ROWS=True)


#%% CORRECION DE TABLA

sql_modify_table = '''
DROP TABLE IF EXISTS #aux_table
SELECT
	ticket_id,
	titulo,
	case when descripcion is NULL THEN titulo ELSE descripcion END as descripcion,
	fechacreacion,
	fecharesolucion,
	personalasistido,
	personalasignado,
	urgencia,
	impacto,
	prioridad,categoria
INTO #aux_table
FROM dbo.Tickets;

DROP TABLE IF EXISTS dbo.Tickets
SELECT *
INTO
dbo.Tickets
FROM
	#aux_table;

DROP TABLE #aux_table;
'''.strip()

obj_database_sqlserver.COMMIT_TABLE(sql_modify_table)
obj_database_sqlserver.Close_db()



#%%
