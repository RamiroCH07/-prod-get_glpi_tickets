from integrate_class import integrate

source = ['10.1.12.222','glpi-empty','bihelpdesk','rb1hd2023']
destination = ['10.1.12.46','Pruebas1','sa','123456SQLserver'] 
obj_integrate = integrate(source,destination)

if __name__ == '__main__':
    obj_integrate.FINAL_JOB()