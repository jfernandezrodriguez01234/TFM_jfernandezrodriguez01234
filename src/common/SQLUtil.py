'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from sqlalchemy import create_engine
import io
import pandas as pd 

class SQLUtil(object):
    '''
    classdocs
    '''
    DB_HOST='localhost'
    DB_PORT='5432'
    DB_NAME='tfm_jfernandez_sqldb'
    DB_USERNAME='postgres'
    DB_PASSWORD='iaaw7nfiOK8jGhHB'
    CADENA_CONEXION='postgresql://'+DB_USERNAME+':'+DB_PASSWORD+'@'+DB_HOST+':'+DB_PORT+'/'+DB_NAME
    
    SCHEMA_NAME_CONSOLIDADO='consolidado'
    
    CADENA_CONEXION_JDBC='jdbc:postgresql://'+DB_HOST+':'+DB_PORT+'/'+DB_NAME
    JDBC_DRIVER ='org.postgresql.Driver'
    
    engine = create_engine(CADENA_CONEXION)
    SQL_QUERIES = {
        "cotizaciones_empresas" : '''
            (select * from empresas_cotizacion ec 
            join cotizaciones_empresas ce on ec.id = ce.id_empresa
            where ce.fecha >= '2015-01-01 00:00:00' ) cotizaciones
        ''',
        
        "datos_covid":'''
            (select cc."Country/Region", 
             cc."fecha", 
             sum(cc.numero) as confirmados,
             sum(cr.numero) as recuperados,
             sum(cf.numero) as fallecidos
            from covid_19_confirmados cc
            left join covid_19_recuperados cr on cc."Country/Region" = cr."Country/Region" and cc."fecha" = cr."fecha" and coalesce(cc."Province/State",'vacio') = coalesce(cr."Province/State",'vacio')
            left join covid_19_fallecidos cf on cc."Country/Region" = cf."Country/Region" and cc."fecha" = cf."fecha" and coalesce(cc."Province/State",'vacio') = coalesce(cf."Province/State",'vacio')
            group by cc."Country/Region", cc."fecha" 
            having sum(cc.numero) > 1000 
            order by cc."fecha" desc, cc."Country/Region") covid_data
        ''',
        
        "registro_defunciones":'''
        (select nombre_ambito as comunidad, nombre_sexo as sexo, nombre_gedad as grupo_edad, 
            fecha_defuncion, defunciones_observadas, defunciones_esperadas, defunciones_esperadas_q01, defunciones_esperadas_q99
            from public.defunciones_momo dm 
            where ambito = 'ccaa'
            and nombre_sexo != 'todos'
            and nombre_gedad != 'todos'
        ) registro_defunciones
        ''',

        "trafico_calle_30_madrid":'''
        (select *
            from trafico_calle_30_madrid 
        ) trafico_calle_30_madrid
        ''',
        
        "comunidades_autonomas":'''
        (select *
            from comunidades_autonomas 
        ) comunidades_autonomas
        ''',
        "calidad_aire":'''
        (select dca."PROVINCIA", dca."MUNICIPIO",dca."FECHA", dca."MEDIODA", ec.* from datos_diarios_calidad_aire dca
        join estaciones_control ec on dca."ESTACION"= ec."CODIGO_CORTO"
        where dca."MAGNITUD" = 8 
        and dca."VALIDO" = true
        ) calidad_aire
        '''
    
        }
    
    
    def __init__(self, params):
        '''
        Constructor
        '''
    
    @staticmethod 
    def pandasToTable( df, table_name):
        df.head(0).to_sql(table_name, SQLUtil.engine, if_exists='replace',index=False) #truncates the table
        conn = SQLUtil.engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cur.copy_from(output, table_name, null="") # null values become ''
        conn.commit()

    @staticmethod
    def tableToPandas(table_name):
        return pd.read_sql_table(table_name,SQLUtil.engine)
    
    @staticmethod
    def readSparkDf(spark, table_name):
        return spark.read \
            .format("jdbc") \
            .option("url", SQLUtil.CADENA_CONEXION_JDBC) \
            .option("dbtable", SQLUtil.SQL_QUERIES[table_name]) \
            .option("user", SQLUtil.DB_USERNAME) \
            .option("password", SQLUtil.DB_PASSWORD) \
            .option("driver", SQLUtil.JDBC_DRIVER) \
            .load()
    
    @staticmethod
    def writeSparkDf(df, table_name, overwrite):
        if (overwrite):
            mode = "overwrite"
        table_name_consolidado = SQLUtil.SCHEMA_NAME_CONSOLIDADO + "." + table_name
        properties = {"user": SQLUtil.DB_USERNAME,"password": SQLUtil.DB_PASSWORD,"driver": SQLUtil.JDBC_DRIVER}
        df.write.jdbc(url=SQLUtil.CADENA_CONEXION_JDBC, table=table_name_consolidado, mode=mode, properties=properties)
