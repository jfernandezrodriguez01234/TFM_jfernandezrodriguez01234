'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import pandas as pd 
from datetime import date, datetime


class EnvironmentDataCrawler(object):
    '''
    classdocs
    '''
    # Declaracion de constantes
    URL_ESTACIONES_CONTROL = 'https://datos.madrid.es/egob/catalogo/212629-1-estaciones-control-aire.csv'
    URL_DATOS_DIARIOS = 'https://datos.madrid.es/egob/catalogo/201410-10306609-calidad-aire-diario.csv'

    data_estaciones_control = None
    data_datos_diarios = None
    
    def __init__(self):
        '''
        Constructor
        '''
    
    def extract(self):
        # Se obtienen los datos de recuperados, fallecidos y confirmados
        self.data_estaciones_control = pd.read_csv(EnvironmentDataCrawler.URL_ESTACIONES_CONTROL, sep=';', encoding='cp1252') 
        self.data_datos_diarios = pd.read_csv(EnvironmentDataCrawler.URL_DATOS_DIARIOS, sep=';', encoding='cp1252')
        
    def transform(self):
        self.data_datos_diarios = self.data_datos_diarios.melt(id_vars=['PROVINCIA', 'MUNICIPIO', 'ESTACION', 'MAGNITUD', 'PUNTO_MUESTREO', 'ANO', 'MES'],
                                            var_name="DATO",
                                            value_name="VALOR")
            
        columnas = ['PROVINCIA','MUNICIPIO','ESTACION','MAGNITUD','PUNTO_MUESTREO','FECHA','VALIDO','MEDIODA']
        df = pd.DataFrame(columns=columnas)
        for i, p in self.data_datos_diarios.iterrows():
            valido = None
            medida = None
            if 'D' in p['DATO']:
                medida = p['VALOR']
            if 'V' in p['DATO']:
                valido = 'V' in p['VALOR']
        
            diaStr = p['DATO'].replace('D','').replace('V','')
            try:  
                fecha = datetime.strptime(str(p['ANO'])+'/' +str(p['MES'])+'/'+diaStr, '%Y/%m/%d').date()
            except ValueError:
                continue
            
            data = [[p['PROVINCIA'],p['MUNICIPIO'],p['ESTACION'],p['MAGNITUD'],p['PUNTO_MUESTREO'], fecha ,valido, medida]]
            df.dtypes
            df2 = pd.DataFrame(data , columns=columnas)
            df = df.append(df2,ignore_index=True)
            
        join_columns = ['PROVINCIA','MUNICIPIO','ESTACION','MAGNITUD','PUNTO_MUESTREO','FECHA']
        new_df = df[df['VALIDO'].notnull()].merge(df[df['MEDIODA'].notnull()],left_on=join_columns, right_on = join_columns)
        new_df = new_df.rename(columns={"VALIDO_x": "VALIDO", "MEDIODA_y": "MEDIODA"})[columnas].drop_duplicates()
        
        new_df['PROVINCIA'] = new_df['PROVINCIA'].astype('int32') 
        new_df['MUNICIPIO'] = new_df['MUNICIPIO'].astype('int32')
        new_df['ESTACION'] = new_df['ESTACION'].astype('int32')
        new_df['MAGNITUD'] = new_df['MAGNITUD'].astype('int32')
        new_df['FECHA'] = new_df['FECHA'].astype('datetime64')
        new_df['VALIDO'] = new_df['VALIDO'].astype('bool')
        new_df['MEDIODA'] = new_df['MEDIODA'].astype('float32')
        
        self.data_datos_diarios = new_df
        
        self.data_estaciones_control['CODIGO_CORTO'] = self.data_estaciones_control['CODIGO_CORTO'].astype('int32')
        
    def load(self):
        from common.SQLUtil import SQLUtil
        SQLUtil.pandasToTable(self.data_datos_diarios,'datos_diarios_calidad_aire')
        SQLUtil.pandasToTable(self.data_estaciones_control, 'estaciones_control')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()

