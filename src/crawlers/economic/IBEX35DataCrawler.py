'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
from datetime import date,datetime
import logging
import time

from alpha_vantage.timeseries import TimeSeries

from common.SQLUtil import SQLUtil
import pandas as pd 


class IBEX35DataCrawler(object):
    '''
    classdocs
    '''
    #Declaración de constantes
    ALPHA_VANTAGE_KEY = 'JVWOPOS96DHQWLXY'

    empresasCotizacion = None
    cotizaciones = None
    
    def __init__(self):
        '''
        Constructor
        '''
        self.empresasCotizacion = SQLUtil.tableToPandas('empresas_cotizacion')
        
    def extract(self):
        ts = TimeSeries(key=IBEX35DataCrawler.ALPHA_VANTAGE_KEY, output_format='pandas')
        self.cotizaciones = pd.DataFrame()
        
        for index, row in self.empresasCotizacion.iterrows():
            logging.debug("Se obtienen las cotizaciones para {} - {}".format(row['companhia'],row['id']))
            try:
                data, meta_data = ts.get_daily_adjusted(symbol=row['simbolo'], outputsize='full')
                data['fecha'] = data.index
                data.loc[:,'id_empresa'] = row['id']
                self.cotizaciones = self.cotizaciones.append(data)
                logging.debug("Cotizaciones recogidas OK ")
            except:
                logging.error("Ha ocurrido un error realizando la consulta, se continua el proceso. ")
            #El API gratuito de alphavantage permite 5 llamadas por minuto, se duerme durante 13 segundos para no superar este límite.
            time.sleep(13)
        
        
    def transform(self):
        self.cotizaciones = self.cotizaciones.rename(columns={"1. open": "apertura", "2. high": "maximo", "3. low": "minimo", 
                                                              "4. close": "cierre", "5. adjusted close": "cierre_ajustado",
                                                              "6. volume": "volumen","7. dividend amount": "dividendos",
                                                              "8. split coefficient": "coeficiente"})
        self.cotizaciones['fecha'] = self.cotizaciones['fecha'].apply(lambda x : datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d').date()).astype('datetime64')
        
    def load(self):
        from common.SQLUtil import SQLUtil
        SQLUtil.pandasToTable(self.cotizaciones,'cotizaciones_empresas')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()
