'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import datetime, time  
import urllib.request
from pyspark.sql.types import IntegerType
from common.SQLUtil import SQLUtil
from pyspark.sql.functions import split

class PernoctacionesProcessor(object):
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    source_table = "defunciones_momo"
    source_table_aux = "comunidades_autonomas"
    
    mapeo_codigos = 'resources/mapeo_comunidades.csv'
        
    df = None
    spark = None
    sc = None
    sql = None 
    
    def __init__(self,spark, sc, sql):
        '''
        Constructor
        '''
        PernoctacionesProcessor.spark = spark
        PernoctacionesProcessor.sc = sc 
        PernoctacionesProcessor.sql = sql
        
    def process(self):
        df_datos_pernoc_ine = PernoctacionesProcessor.spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.PERNOC_INE").load()
        df_datos_pernoc_ine.registerTempTable("PERNOC_INE")
        
        df_datos_pernoc_ine_base = PernoctacionesProcessor.sql.sql('''
        SELECT Nombre as nombre_variable, 
                Unidad.Nombre as unidad, 
                Escala.Nombre as escala, 
                datos.TipoDato.Nombre as tipoDato,
                datos.NombrePeriodo as Periodo,
                datos.Valor as valor
         FROM PERNOC_INE  
              LATERAL VIEW explode(Data) as datos
         WHERE datos.Anyo = 2020
         ''')
         
        split_col = split(df_datos_pernoc_ine_base['nombre_variable'], '\\.')
        df_datos_pernoc_ine_base = df_datos_pernoc_ine_base.withColumn('region', split_col.getItem(0))
        df_datos_pernoc_ine_base = df_datos_pernoc_ine_base.withColumn('origen_viajero', split_col.getItem(2))
        
        split_col = split(df_datos_pernoc_ine_base['Periodo'], 'M')
        df_datos_pernoc_ine_base = df_datos_pernoc_ine_base.withColumn('anho', split_col.getItem(0).cast(IntegerType()))
        df_datos_pernoc_ine_base = df_datos_pernoc_ine_base.withColumn('mes', split_col.getItem(1).cast(IntegerType()))
        
        columns_to_drop = ['nombre_variable', 'escala', 'Periodo']
        df_datos_pernoc_ine_base = df_datos_pernoc_ine_base.select([column for column in df_datos_pernoc_ine_base.columns if column not in columns_to_drop])

        df_datos_pernoc_ine_base.show(20, False)
        SQLUtil.writeSparkDf(df_datos_pernoc_ine_base, "datos_pernoctaciones_ine", True)
        
        df_datos_pernoc_ine_base.unpersist(blocking=True)
#    
        
        


