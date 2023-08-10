from EngineDBPY.DB_SQL_SERVER import DB_SQL_Server

class loader:
    def __init__(self,datos_sql):
        self.obj_database = DB_SQL_Server(
            datos_sql[0],
            datos_sql[1],
            datos_sql[2],
            datos_sql[3])
        
    def STORAGE_DATA_IN_DB(self,df):
        self.obj_database.Connect_db()
        columns = ['ticket_id','titulo','descripcion','fechacreacion','fecharesolucion',
                   'personalasistido','personalasignado','urgencia','impacto','prioridad','categoria']

        table_name = 'Tickets'

        self.obj_database.STORAGE_ROWS_db('',columns,
                                               df.values,
                                               table_name,
                                               ADD_NEW_ROWS=True)
        ## ULTIMA MOIFICACION
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

        self.obj_database.COMMIT_TABLE(sql_modify_table)

          
        self.obj_database.Close_db()
        

        
        
        
