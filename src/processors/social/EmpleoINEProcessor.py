'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''

from common.SQLUtil import SQLUtil


class EmpleoINEProcessor(object):
    
    '''
    classdocs
    '''
    
    def __init__(self, spark, sc, sql):
        '''
        Constructor
        '''
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        
    def process(self):
        
       
        df_datos_empleo_ine = self.spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.EMPLEO_INE").load()
        df_datos_empleo_ine.registerTempTable("EMPLEO_INE")
     
        df_datos_empleo_ine_base = self.sql.sql('''
             SELECT Nombre as nombre_variable, 
                    Unidad.Nombre as unidad, 
                    Escala.Nombre as escala, 
                    datos.TipoDato.Nombre as tipoDato,
                    datos.NombrePeriodo as Periodo,
                    datos.Valor as valor
             FROM EMPLEO_INE  
                  LATERAL VIEW explode(Data) as datos
             WHERE Nombre like '%Dato base%'
         ''')
      
        df_datos_empleo_ine_base.show(20, False)
        SQLUtil.writeSparkDf(df_datos_empleo_ine_base, "datos_empleo_ine", True)
        
        


