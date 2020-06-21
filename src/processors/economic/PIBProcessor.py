'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from pyspark.sql.types import StringType
from common.SQLUtil import SQLUtil
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
import datetime

class PIBProcessor(object):
    '''
    classdocs
    '''
       
    df = None
    spark = None
    sc = None
    sql = None 
    
    def getCategoryFromNombre(self, strNombre):
        arrNombres = strNombre.split('.')
        return arrNombres[2] 
    
    def getSubCategoryFromNombre(self, strNombre):
        arrNombres = strNombre.split('.')
        if (len(arrNombres) == 6 ):
            return None
        return arrNombres[3]         
        

    def getBaseComparacion(self, strNombre):
        arrNombres = strNombre.split('.')
        if (len(arrNombres) == 6 ):
            return arrNombres[3]
        else:
            return arrNombres[4]
       
    
    def __init__(self,spark, sc, sql):
        '''
        Constructor
        '''
        PIBProcessor.spark = spark
        PIBProcessor.sc = sc 
        PIBProcessor.sql = sql
        
    def process(self):
        df_datos_PIB_ine = PIBProcessor.spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.PIB_INE").load()
        df_datos_PIB_ine.registerTempTable("PIB_INE")
         
        df_datos_PIB_ine_base = PIBProcessor.sql.sql('''
            SELECT Nombre as nombre_variable, 
                   Unidad.Nombre as unidad, 
                   Escala.Nombre as escala, 
                   datos.TipoDato.Nombre as povDef,
                   datos.Fecha as fecha_ini_periodo,
                   add_months(datos.Fecha,3) as fecha_fin_periodo,
                   datos.NombrePeriodo as Periodo,
                   datos.Valor as valor
            FROM PIB_INE  
                 LATERAL VIEW explode(Data) as datos
            ''')
        
        get_category_udf = udf(lambda x: self.getCategoryFromNombre(x), StringType())
        get_subcategory_udf = udf(lambda x: self.getSubCategoryFromNombre(x), StringType())
        get_base_comparacion_udf = udf(lambda x: self.getBaseComparacion(x), StringType())
        
        split_col = split(df_datos_PIB_ine_base['nombre_variable'], '\\.')
        df_datos_PIB_ine_base = df_datos_PIB_ine_base.withColumn('ambito', split_col.getItem(0))
        df_datos_PIB_ine_base = df_datos_PIB_ine_base.withColumn('tipo_dato', split_col.getItem(1))
        df_datos_PIB_ine_base = df_datos_PIB_ine_base.withColumn('categoria', get_category_udf(df_datos_PIB_ine_base.nombre_variable))
        df_datos_PIB_ine_base = df_datos_PIB_ine_base.withColumn('subCategoria', get_subcategory_udf(df_datos_PIB_ine_base.nombre_variable))
        df_datos_PIB_ine_base = df_datos_PIB_ine_base.withColumn('base_comparado', get_base_comparacion_udf(df_datos_PIB_ine_base.nombre_variable))
        
       
        columns_to_drop = ['nombre_variable']
        df_datos_PIB_ine_base = df_datos_PIB_ine_base\
            .select([column for column in df_datos_PIB_ine_base.columns if column not in columns_to_drop])\
            .where(df_datos_PIB_ine_base.tipo_dato == ' Datos ajustados de estacionalidad y calendario')
        
        df_datos_PIB_ine_base.show(20, False)
        
        SQLUtil.writeSparkDf(df_datos_PIB_ine_base, "datos_pib_ine", True)
        
        df_datos_PIB_ine_base.unpersist(blocking=True)
      
        