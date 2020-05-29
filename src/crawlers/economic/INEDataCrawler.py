'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import requests
import pandas as pd
import logging
import pprint
from datetime import datetime
from common.MongoUtil import MongoLoad
import time

class INEDataCrawler(object):
    '''
    classdocs
    '''
    #Declaraci�n de constantes
    URL_PLANTILLA_TABLA = 'http://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{codigo}?det=2'

    #INE - Pernoctaciones
    CODIGO_INE_PERNOCTACIONES = "2044"
    MONGO_COLLECTION_PERNOC="PERNOC_INE"

    #INE_OTROS_ECONOMICO - COMPONENTES DEL PIB
    INE_ECONOMICO_TABLA = "30678"
    MONGO_COLLECTION_PIB="PIB_INE"
    
    #INE_OTROS_ECONOMICO - EMPLEO
    INE_EMPLEO_TABLA = "30684"
    MONGO_COLLECTION_EMPLEO="EMPLEO_INE"

    
    json_pernoctaciones=None
    json_pib=None
    json_empleo=None
    
    pp = pprint.PrettyPrinter(indent=4)
    
    def __init__(self):
        '''
        Constructor
        '''

    def extract(self):
        url_pernoctaciones = self.URL_PLANTILLA_TABLA.format(codigo=self.CODIGO_INE_PERNOCTACIONES)
        url_datos_PIB = self.URL_PLANTILLA_TABLA.format(codigo=self.INE_ECONOMICO_TABLA)
        url_datos_empleo = self.URL_PLANTILLA_TABLA.format(codigo=self.INE_EMPLEO_TABLA)
        
        self.json_pernoctaciones = requests.get(url_pernoctaciones).json()
        if isinstance(self.json_pernoctaciones, dict):
            logging.error('Error obteniendo la información de pernoctaciones. Se para el proceso 30 segundos y se reintenta')
            time.sleep(30)
            self.json_pernoctaciones = requests.get(url_pernoctaciones).json()    

        self.json_pib = requests.get(url_datos_PIB).json()
        if isinstance(self.json_pib, dict):
            logging.error('Error obteniendo la información de componentes del PIB. Se para el proceso 30 segundos y se reintenta')
            time.sleep(30)
            self.json_pib = requests.get(url_datos_PIB).json()
            
        self.json_empleo = requests.get(url_datos_empleo).json()
        if isinstance(self.json_pib, dict):
            logging.error('Error obteniendo la información de empleo. Se para el proceso 30 segundos y se reintenta')
            time.sleep(30)
            self.json_empleo = requests.get(url_datos_empleo).json()
    
    #Método quie convierte las fechas de unix timestamp a fechas interpretables por MongoDb.
    @staticmethod
    def replaceFecha(obj):
        if isinstance(obj, list):
            i = 0
            for elemento in obj:
                obj[i] = INEDataCrawler.replaceFecha(elemento)
                i+=1 
        else:
            for k, v in obj.items():
                if isinstance(v, dict) or isinstance(v, list):
                    obj[k] = INEDataCrawler.replaceFecha(v)
                if k == 'Fecha':
                    obj[k] = pd.to_datetime(v, unit='ms', utc=True).tz_convert('Europe/Madrid')  
        return obj
                    
    def transform(self):
        #Todos los registros del INE tienen sus fechas en formato UNIX TIMESTAMP. Las convertimos a formato fecha
        self.json_pernoctaciones = INEDataCrawler.replaceFecha(self.json_pernoctaciones)
        self.json_pib = INEDataCrawler.replaceFecha(self.json_pib)
        self.json_empleo = INEDataCrawler.replaceFecha(self.json_empleo)
        

    def load(self):
        #La carga se realiza en colecciones 
        try:
            MongoLoad.insertMany(self.json_pernoctaciones,INEDataCrawler.MONGO_COLLECTION_PERNOC , True)
        except Exception:
            logging.error('Error almacenando los datos de pernoctaciones, se continua con el resto ')
        
        try:    
            MongoLoad.insertMany(self.json_pib,INEDataCrawler.MONGO_COLLECTION_PIB , True)
        except Exception:
            logging.error('Error almacenando los datos de componentes del PIB, se continua con el resto ')
        
        try:
            MongoLoad.insertMany(self.json_empleo,INEDataCrawler.MONGO_COLLECTION_EMPLEO , True)
        except Exception:
            logging.error('Error almacenando los datos de empleo, se continua con el resto ')
    
    def exec(self):
        self.extract()
        self.transform()
        self.load()
