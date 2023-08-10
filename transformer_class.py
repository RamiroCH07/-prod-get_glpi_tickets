import numpy as np

class transformer:

    @staticmethod 
    def transform_content_camp(content_camp,title_camp):
        def __map_transform_content(txt):
            if txt.startswith('&lt;div') or txt.startswith('&lt;meta') or txt.startswith('&lt;table') or txt.startswith('<div'):
                txt = 'mail'
            else:
                txt_split = txt.split(';')
                if len(txt_split) > 1:
                    txt = txt_split[2].strip('.&lt')
            return txt
        
        new_content_camp = content_camp.map(__map_transform_content)
        
        new_content_camp = np.where(
            new_content_camp == 'mail',
            title_camp,
            new_content_camp
            )
    
        
        return new_content_camp
    

    
    
    