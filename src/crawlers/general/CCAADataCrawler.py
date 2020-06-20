'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import pandas as pd  
        
class CCAADataCrawler(object):
    '''
    classdocs
    '''
    #Declaración de constantes
    base_url = "https://public.opendatasoft.com/explore/dataset/comunidades-autonomas-espanolas/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"
    
    data = None
    dataframe = None
    
    def __init__(self):
        '''
        Constructor
        '''
    
    def extract(self):
        self.dataframe = pd.read_csv(CCAADataCrawler.base_url, sep=';')
        
        
    def transform(self):
        coordenadas_separadas = self.dataframe["Geo Point"].str.split(",", n = 1, expand = True)
        self.dataframe['latitud'] = coordenadas_separadas[0]
        self.dataframe['longitud'] = coordenadas_separadas[1]
        self.dataframe.drop(columns =["Geo Point"], inplace = True)  
        self.dataframe.head()
        
        
    def load(self):
        from common.SQLUtil import SQLUtil
        SQLUtil.pandasToTable(self.dataframe,'comunidades_autonomas')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()
