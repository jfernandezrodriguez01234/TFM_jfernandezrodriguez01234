'''
Created on 25 abr. 2020

Clase general para el procesamiento de los datos almacenados en la base de datos.

@author: jesus.fernandez
'''
import logging
import findspark
from pyspark.sql import SparkSession, SQLContext 
from pyspark import SparkConf
import pyspark
from common.SQLUtil import SQLUtil
from pyspark.sql.functions import udf
import datetime


# Para lso nombres de las comunidades, si tienen coma, debemos pasar la cadena de después de la coma a 'antes', por ejemplo:
# Rioja, La pasa a ser La Rioja
def transform_com_name(commName):
    x = commName.split(",")
    if len(x) == 1: 
        return commName
    else:
        return (x[1] + " " + x[0]).strip()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s' , level=logging.INFO)
    
    findspark.init()
    config = SparkConf().setMaster("local") \
                         .setAppName("TFMJfernandez")\
                         .set("spark.local.dir", "c:\\tmp\\")\
                         .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
    sc = pyspark.SparkContext(conf=config)
    sql = SQLContext(sc)
    spark = SparkSession(sc)

    df_cotizaciones = SQLUtil.readSparkDf(spark, "cotizaciones_empresas")  
    SQLUtil.writeSparkDf(df_cotizaciones, "cotizaciones", True)
    df_cotizaciones.unpersist(blocking=True)
 
    df_datos_covid = SQLUtil.readSparkDf(spark, "datos_covid")
    SQLUtil.writeSparkDf(df_datos_covid, "datos_covid", True)
    df_datos_covid.unpersist(blocking=True)
     
    df_datos_momo = SQLUtil.readSparkDf(spark, "datos_covid")
    SQLUtil.writeSparkDf(df_datos_covid, "datos_covid", True)
    df_datos_covid.unpersist(blocking=True)
     
    df_registro_defunciones = SQLUtil.readSparkDf(spark, "registro_defunciones") 
         
    from pyspark.sql.types import StringType
    udf_communityName = udf(transform_com_name, StringType())
    df_registro_defunciones = df_registro_defunciones.withColumn('comunidad', udf_communityName(df_registro_defunciones.comunidad))
     
    fecha_corte = datetime.date(2020, 1, 1)
    df_registro_defunciones = df_registro_defunciones.where(df_registro_defunciones.fecha_defuncion >= fecha_corte)
 
    SQLUtil.writeSparkDf(df_registro_defunciones, "registro_defunciones", True)
    df_registro_defunciones.unpersist(blocking=True)

    df_datos_empleo_ine = spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.EMPLEO_INE").load()
    df_datos_empleo_ine.registerTempTable("EMPLEO_INE")
   
    df_datos_empleo_ine_base = sql.sql('''
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
    
    df_datos_pernoc_ine = spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.PERNOC_INE").load()
    df_datos_pernoc_ine.registerTempTable("PERNOC_INE")
   
    df_datos_pernoc_ine_base = sql.sql('''
        SELECT Nombre as nombre_variable, 
               Unidad.Nombre as unidad, 
               Escala.Nombre as escala, 
               datos.TipoDato.Nombre as tipoDato,
               datos.NombrePeriodo as Periodo,
               datos.Valor as valor
        FROM PERNOC_INE  
             LATERAL VIEW explode(Data) as datos
        ''')
    
    df_datos_pernoc_ine_base.show(20, False)
    SQLUtil.writeSparkDf(df_datos_pernoc_ine_base, "datos_pernoctaciones_ine", True)
    
    df_datos_PIB_ine = spark.read.format("mongo").option("uri", "mongodb://localhost/TFM_Jfernandez.PIB_INE").load()
    df_datos_PIB_ine.registerTempTable("PIB_INE")
   
    df_datos_PIB_ine_base = sql.sql('''
        SELECT Nombre as nombre_variable, 
               Unidad.Nombre as unidad, 
               Escala.Nombre as escala, 
               datos.TipoDato.Nombre as tipoDato,
               datos.Fecha as fecha_ini_periodo,
               add_months(datos.Fecha,3) as fecha_fin_periodo,
               datos.NombrePeriodo as Periodo,
               datos.Valor as valor,
               CASE 
                 WHEN Nombre like '%Dato base%' THEN 'Dato base' 
                 WHEN Nombre like '%Variación trimestral%' THEN 'Variación trimestral' 
                 WHEN Nombre like '%Variación anual%' THEN 'Variación anual'
                 ELSE null 
               END as tipo_dato
        FROM PIB_INE  
             LATERAL VIEW explode(Data) as datos
        WHERE Nombre like '%Datos ajustados%'
        ''')
    
    df_datos_PIB_ine_base.show(20, False)
    SQLUtil.writeSparkDf(df_datos_PIB_ine_base, "datos_pib_ine", True)
    
