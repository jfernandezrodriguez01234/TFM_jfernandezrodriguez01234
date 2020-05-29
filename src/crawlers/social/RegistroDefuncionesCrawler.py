'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import datetime, time  
import urllib.request

import pandas as pd 


class RegistroDefuncionesCrawler(object):
    '''
    classdocs
    '''
    #Declaración de constantes
    base_url = "https://momo.isciii.es/public/momo/data"
    #El header es necesario para 'simular' el acceso desde un navegador web
    http_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}
    
    df = None
    
    def __init__(self):
        '''
        Constructor
        '''
    
    def extract(self):
        req = urllib.request.Request(RegistroDefuncionesCrawler.base_url,headers=RegistroDefuncionesCrawler.http_headers)
        file = urllib.request.urlopen(req)
        self.df = pd.read_csv(file)
        file.close;
        
        
    def transform(self):
        #TODO: Analizar el mejor lugar para almacenar la fecha de actualización
     
        self.df['ambito'] = self.df['ambito'].astype('category')
        self.df['cod_ambito'] = self.df['cod_ambito'].astype('category')
        self.df['cod_ine_ambito'] = self.df['cod_ine_ambito'].astype('category')
        self.df['nombre_ambito'] = self.df['nombre_ambito'].astype('category')
        self.df['cod_sexo'] = self.df['cod_sexo'].astype('category')
        self.df['nombre_sexo'] = self.df['nombre_sexo'].astype('category')
        self.df['cod_gedad'] = self.df['cod_gedad'].astype('category')
        self.df['nombre_gedad'] = self.df['nombre_gedad'].astype('category')
        self.df['fecha_defuncion'] = self.df['fecha_defuncion'].apply(lambda x : datetime.datetime.strptime(x, '%Y-%m-%d').date()).astype('datetime64')
        
    def load(self):
        from common.SQLUtil import SQLUtil
        SQLUtil.pandasToTable(self.df,'defunciones_momo')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()
