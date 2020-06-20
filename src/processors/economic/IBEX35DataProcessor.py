'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from common.SQLUtil import SQLUtil

class IBEX35DataProcessor(object):
    
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    IBEX35_data_table = "cotizaciones_empresas"
    
    df = None
    spark = None
    sc = None
    sql = None 
    
    def __init__(self,fecha_corte, spark, sc, sql):
        '''
        Constructor
        '''
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        
    def process(self):
        df_cotizaciones = SQLUtil.readSparkDf(self.spark, IBEX35DataProcessor.IBEX35_data_table)  
        SQLUtil.writeSparkDf(df_cotizaciones, "cotizaciones", True)
        df_cotizaciones.unpersist(blocking=True)
        
        


