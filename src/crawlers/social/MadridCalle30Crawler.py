'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import urllib.request
import xmltodict
import pandas as pd 
import datetime,time  
        
class MadridCalle30Crawler(object):
    '''
    classdocs
    '''
    #Declaración de constantes
    base_url = "http://www.mc30.es/images/xml/historicousuarios.xml"
    #El header es necesario para 'simular' el acceso desde un navegador web
    http_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}
    
    data = None
    dataframe = None
    
    def __init__(self):
        '''
        Constructor
        '''
    
    def extract(self):
        req = urllib.request.Request(MadridCalle30Crawler.base_url,headers=MadridCalle30Crawler.http_headers)
        file = urllib.request.urlopen(req)
        data = file.read()
        file.close()
        self.data = xmltodict.parse(data)
        
        
    def transform(self):
        #TODO: Analizar el mejor lugar para almacenar la fecha de actualización
        fechaActualizacion = self.data['HistoricoUsuarios']['FechaActualizacion']['campo']['valor']

        df = pd.DataFrame(self.data['HistoricoUsuarios']['Historico'])

        df.head()
        def timeToSeconds(text):
            if text is None:
                return None
            tt= time.strptime(text,"%M min. %S seg.")
            return tt.tm_min * 60 + tt.tm_sec
        
        #se aplican operaciones de limpieza:
        df['Fecha'] = df['Fecha'].apply(lambda x : datetime.datetime.strptime(x, '%d/%m/%Y').date()).astype('datetime64')
        df['UsuariosCalle30'] =  df['UsuariosCalle30'].apply(lambda x : x if x is None else x.replace(' veh','')).astype('int32')
        df['vehxKmTotales'] =  df['vehxKmTotales'].apply(lambda x : x if x is None else x.split(",")[0].replace(' veh x Km','')).astype('int32') 
        df['vehxKmRamales'] =  df['vehxKmRamales'].apply(lambda x : x if x is None else x.split(",")[0].replace(' veh x Km','')).astype('int32') 
        df['velocidadMedia'] =  df['velocidadMedia'].apply(lambda x : x if x is None else x.replace(' Km/hora','').replace(',','.')).astype('float32') 
        df['distanciaMediaRecorrida'] =  df['distanciaMediaRecorrida'].apply(lambda x : x if x is None else x.replace(' metros','')).astype('int32')
        df['tiempoMediodeRecorrido'] =  df['tiempoMediodeRecorrido'].apply(lambda x : timeToSeconds(x) ).astype('float32') 
        self.dataframe = df
        
        
    def load(self):
        from common.SQLUtil import SQLUtil
        SQLUtil.pandasToTable(self.dataframe,'trafico_calle_30_madrid')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()
