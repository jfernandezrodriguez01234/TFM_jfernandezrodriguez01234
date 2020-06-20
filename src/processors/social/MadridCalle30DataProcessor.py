'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from common.SQLUtil import SQLUtil

class MadridCalle30DataProcessor(object):
    
    '''
    classdocs
    '''
    TABLA_TRAFICO_MC30 = "trafico_calle_30_madrid"
    
    def __init__(self, spark, sc, sql,fecha_corte):
        '''
        Constructor
        '''
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        self.fecha_corte = fecha_corte
        
    def process(self):
        df_madrid_calle30 = SQLUtil.readSparkDf(self.spark, MadridCalle30DataProcessor.TABLA_TRAFICO_MC30)
        df_madrid_calle30 = df_madrid_calle30.where(df_madrid_calle30.Fecha >= self.fecha_corte)
        SQLUtil.writeSparkDf(df_madrid_calle30, MadridCalle30DataProcessor.TABLA_TRAFICO_MC30, True)
        df_madrid_calle30.unpersist(blocking=True)
       
        
        


