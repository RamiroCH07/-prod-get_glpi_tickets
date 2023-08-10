from EngineDBPY.DB_SQL_SERVER import DB_SQL_Server
from EngineDBPY.DB_MYSQL import DB_MYSQL
import pandas as pd
#%%
class estractor:
    def __init__(self, datos_sql,datos_mysql):
        datos_sql = datos_sql
        datos_mysql = datos_mysql
        self.obj_database_sql_server = DB_SQL_Server(
            datos_sql[0],
            datos_sql[1],
            datos_sql[2],
            datos_sql[3])
        self.obj_database_mysql = DB_MYSQL(
            datos_mysql[0],
            datos_mysql[1],
            datos_mysql[2],
            datos_mysql[3])
        self.obj_database_mysql.Connect_db()
        self.obj_database_sql_server.Connect_db()
        f = open('query_count_tickets_mysql.txt','r')
        query_count_tickets_mysql = f.read()
        f.close()
        f = open('query_count_tickets_sql.txt','r')
        query_count_tickets_sql = f.read()
        f.close()
        self.len_sql = self.obj_database_sql_server.GET_ONE_ROW_db(query_count_tickets_sql)
        self.len_mysql = self.obj_database_mysql.GET_ONE_ROW_db(query_count_tickets_mysql)
        self.obj_database_mysql.Close_db()
        self.obj_database_sql_server.Close_db()

        
    def had_update(self):
        if self.len_mysql - self.len_sql > 0:
            return True
        else:
            return False
    
    def GET_ADDED_TICKETS(self):
        num_tickets_added = self.len_mysql - self.len_sql
        query_get_added_tickets = '''
        SELECT * FROM
            (SELECT 
            	ticked_id,
                titulo,
                descripcion,
                fecha_creacion,
                fecha_resolucion,
                concat(nombre_personal_asistido,' ',apellido_personal_asistido) personal_asistido,
                concat(nombre_personal_asistio,' ',apellido_personal_asistio) personal_asignado,
                nivel_urgencia,
                nivel_impacto,
                nivel_prioridad,
                categoria
            
            FROM 
            (SELECT 
            	final_tick.ticked_id,
                final_tick.titulo,
                final_tick.descripcion,
                final_tick.fecha_creacion,
                final_tick.fecha_resolucion,
                us_asist.firstname as nombre_personal_asistido,
                us_asist.realname as apellido_personal_asistido,
                us_asisted.firstname as nombre_personal_asistio,
                us_asisted.realname as apellido_personal_asistio,
                final_tick.nivel_urgencia,
                final_tick.nivel_impacto,
                final_tick.nivel_prioridad,
                cat.name as categoria
            
            FROM
            -- CONSULTA QUE NOS RETORNA LOS REGISTROS DE LOS TICKETS CON EL ID DEL USUARIOS ASISTIDOS Y LOS QUE ASISTIERON
            (SELECT 
            	tick.id as ticked_id,
                tick.name as titulo,
                tick.content as descripcion,
                tick.date as fecha_creacion,
                tick.solvedate as fecha_resolucion,
                tick_u_asist.users_id as usuario_asistido_id,
                tick_u_asisted.users_id as usuario_asistio_id,
                tick.urgency as nivel_urgencia,
                tick.impact as nivel_impacto,
                tick.priority as nivel_prioridad,
                tick.itilcategories_id as categoria_id
            FROM
            	glpi_tickets tick
            LEFT JOIN
            -- CONSULTA DE LOS IDS DE LOS TICKETS Y EL ID USUARIO QUE FUE ASISTIDO
            (SELECT 
            	tickets_id,
                MAX(users_id) as users_id
            FROM 
            (SELECT *
            FROM
            	glpi_tickets_users
            WHERE 
            	TYPE = 1) S_Q
            GROUP BY 
            	tickets_id) tick_u_asist 
            ON
            	tick.id = tick_u_asist.tickets_id
            LEFT JOIN
            -- CONSULTA DE LOS IDS DE LOS TICKETS Y EL ID USUARIO QUE ASISTIÓ 
            (SELECT 
            	tickets_id,
                MAX(users_id) as users_id
            FROM 
            (SELECT *
            FROM
            	glpi_tickets_users
            WHERE 
            	TYPE = 2) S_Q
            GROUP BY 
            	tickets_id) tick_u_asisted
            ON
            	tick.id = tick_u_asisted.tickets_id) final_tick
            LEFT JOIN 
            	glpi_users us_asist
            ON 
            	us_asist.id = final_tick.usuario_asistido_id
            LEFT JOIN 
            	glpi_users us_asisted
            ON
            	us_asisted.id = final_tick.usuario_asistio_id 
            LEFT JOIN 
            	glpi_itilcategories cat
            ON
            	cat.id = final_tick.categoria_id) ticked
            ORDER BY fecha_creacion DESC ) ultimate_ticket 
            ORDER BY fecha_creacion desc
            LIMIT {}
        '''.strip().format(num_tickets_added)
        self.obj_database_mysql.Connect_db()
        rows = self.obj_database_mysql.GET_ROWS_db(query_get_added_tickets)
        self.obj_database_mysql.Close_db()
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
                
            
        return df
            
        
        