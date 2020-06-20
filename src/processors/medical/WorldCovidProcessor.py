'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from common.SQLUtil import SQLUtil

class WorldCovidProcessor(object):
    
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    datos_covid_table = "datos_covid"
    
    df = None
    spark = None
    sc = None
    sql = None 
    
    def __init__(self, spark, sc, sql):
        '''
        Constructor
        '''
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        
    def process(self):
        df_datos_covid = SQLUtil.readSparkDf(self.spark, WorldCovidProcessor.datos_covid_table)
        SQLUtil.writeSparkDf(df_datos_covid, WorldCovidProcessor.datos_covid_table, True)
        df_datos_covid.unpersist(blocking=True)
