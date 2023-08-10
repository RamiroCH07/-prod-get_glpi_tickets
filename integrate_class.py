from estractor_class import estractor
from transformer_class import transformer as tr
from loader_class import loader

class integrate:
    
    def __init__(self,db_source,db_destination):
        self.obj_estracter = estractor(db_destination, db_source)
        self.obj_loader = loader(db_destination)
    
    def __had_update(self):
        return self.obj_estracter.had_update()
    def __extract_data(self):
        return self.obj_estracter.GET_ADDED_TICKETS()
    def __transform_data(self,df):
        df['DESCRIPCION'] = tr.transform_content_camp(df['DESCRIPCION'],df['TITULO'])
    def __load_data(self,df):
        self.obj_loader.STORAGE_DATA_IN_DB(df)
        
    def FINAL_JOB(self):
        if self.__had_update():
            print("SE DETECTARON NUEVOS REGISTROS")
            df = self.__extract_data()
            self.__transform_data(df)
            self.__load_data(df)
        else:
            print("NO SE DETECTARON NUEVOS REGISTROS")
            
            
    
    
        