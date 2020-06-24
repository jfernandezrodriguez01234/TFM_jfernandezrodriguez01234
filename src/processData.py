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
import datetime

def initAll():
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s' , level=logging.INFO)
    fecha_corte = datetime.date(2020, 1, 1)
    findspark.init()
    config = SparkConf().setMaster("local") \
                         .setAppName("TFMJfernandez")\
                         .set("spark.local.dir", "c:\\tmp\\")\
                         .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
    sc = pyspark.SparkContext(conf=config)
    sql = SQLContext(sc)
    spark = SparkSession(sc)
    return fecha_corte, sc, spark, sql

def closeAll(spark):
    spark.stop()

if __name__ == '__main__':
    
    fecha_corte, sc, spark, sql = initAll()
    
    logging.info('Inicio del procesado de los datos de registro de defunciones')
    from processors.social.RegistroDefuncionesProcessor import RegistroDefuncionesProcessor
    rdp = RegistroDefuncionesProcessor(fecha_corte, spark, sc, sql)
    rdp.process()
    logging.info('FIN del procesado de extracción de los datos de registro de defunciones')
    
    logging.info('Inicio del procesado de los datos de cotizaciones de empresas del IBEX')
    from processors.economic.IBEX35DataProcessor import IBEX35DataProcessor
    i35 = IBEX35DataProcessor(spark, sc, sql)
    i35.process()
    logging.info('FIN del procesado de los datos de cotizaciones de empresas del IBEX')

    logging.info('Inicio del procesado de los datos de registro de pernoctaciones en establecimientos hoteleros')
    from processors.social.PernoctacionesProcessor import PernoctacionesProcessor
    pp = PernoctacionesProcessor(spark, sc, sql)
    pp.process()
    logging.info('FIN del procesado de los datos de registro de pernoctaciones en establecimientos hoteleros')

    logging.info('Inicio del procesado de los datos de componentes del PIB ')
    from processors.economic.PIBProcessor import PIBProcessor
    PIBp = PIBProcessor(spark, sc, sql)
    PIBp.process()
    logging.info('FIN del procesado de los datos de componentes del PIB ')
    
    logging.info('Inicio del procesado de los datos de calidad del aire ')
    from processors.environment.EnvironmentProcessor import EnvironmentProcessor
    ENVp = EnvironmentProcessor(spark, sc, sql)
    ENVp.process()
    logging.info('FIN del procesado de los datos de calidad del aire ')

    logging.info('Inicio del procesado de los datos medicos del COVID19')
    from processors.medical.WorldCovidProcessor import WorldCovidProcessor
    WCp = WorldCovidProcessor(spark, sc, sql)
    WCp.process()
    logging.info('FIN del procesado de los datos medicos del COVID19')

    logging.info('Inicio del procesado de los datos de tráfico en MadridCalle30')
    from processors.social.MadridCalle30DataProcessor import MadridCalle30DataProcessor
    MC30p = MadridCalle30DataProcessor(spark, sc, sql,fecha_corte)
    MC30p.process()
    logging.info('FIN del procesado de los datos de tráfico en MadridCalle30')

    logging.info('Inicio del procesado de los datos de empleo del INE')
    from processors.social.EmpleoINEProcessor import EmpleoINEProcessor
    EIp = EmpleoINEProcessor(spark, sc, sql)
    EIp.process()
    logging.info('FIN del procesado de los datos de empleo del INE')
    
    closeAll(spark)
    
