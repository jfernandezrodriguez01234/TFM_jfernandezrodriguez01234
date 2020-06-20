'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from pyspark.sql.types import FloatType
from common.SQLUtil import SQLUtil


class RegistroDefuncionesProcessor(object):
    
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    source_table = "defunciones_momo"
    source_table_aux = "comunidades_autonomas"
    
    mapeo_codigos = 'resources/mapeo_comunidades.csv'
        
    df = None
    fecha_corte = None
    spark = None
    sc = None
    sql = None 
    
    def __init__(self,fecha_corte, spark, sc, sql):
        '''
        Constructor
        '''
        self.fecha_corte = fecha_corte
        self.spark = spark
        self.sc = sc 
        self.sql = sql
        
    def process(self):
        
       
        df_registro_defunciones = SQLUtil.readSparkDf(self.spark, "registro_defunciones") 
        df_comunidades_autonomas = SQLUtil.readSparkDf(self.spark, "comunidades_autonomas")
        df_mapeos = self.spark.read.load(self.mapeo_codigos,format="csv", sep=";", inferSchema="true", header="true")
        
        df_mapeos.head()

        df_mapeos.show(n=30)
        df_comunidades_autonomas.show(n=30)
        
        df_registro_defunciones.registerTempTable("registro_defunciones")
        df_comunidades_autonomas.registerTempTable("comunidades_autonomas")
        df_comunidades_autonomas\
                    .withColumn("latitud", df_comunidades_autonomas["latitud"].cast(FloatType()))\
                    .withColumn("longitud", df_comunidades_autonomas["longitud"].cast(FloatType()))
        df_mapeos.registerTempTable("mapeos")

        df_defunciones_con_coordenadas = self.sql.sql('''
        SELECT rd.*, ca.*
        FROM registro_defunciones rd
        JOIN mapeos map on rd.comunidad = map.nombre
        JOIN comunidades_autonomas ca on map.codigo = ca.codigo 
        ''')
        df_defunciones_con_coordenadas = df_defunciones_con_coordenadas.where(df_defunciones_con_coordenadas.fecha_defuncion >= self.fecha_corte)
        
        df_defunciones_con_coordenadas = df_defunciones_con_coordenadas\
            .withColumn("latitud", df_defunciones_con_coordenadas["latitud"].cast(FloatType()))\
            .withColumn("longitud", df_defunciones_con_coordenadas["longitud"].cast(FloatType()))
            
        
        SQLUtil.writeSparkDf(df_defunciones_con_coordenadas, "registro_defunciones", True)
        df_registro_defunciones.unpersist(blocking=True)
        
        


