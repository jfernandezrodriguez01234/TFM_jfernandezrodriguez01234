'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from common.SQLUtil import SQLUtil

class EnvironmentProcessor(object):
    
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    source_table = "datos_diarios_calidad_aire"
    source_table_aux = "estaciones_control"
    
    df = None
    fecha_corte = None
    spark = None
    sc = None
    sql = None 
    
    def __init__(self,spark, sc, sql):
        '''
        Constructor
        '''
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        
    def process(self):
        
        df_lecturas = SQLUtil.readSparkDf(self.spark, "calidad_aire") 
        SQLUtil.writeSparkDf(df_lecturas, "calidad_aire", True)
        df_lecturas.unpersist(blocking=True)
        
        


