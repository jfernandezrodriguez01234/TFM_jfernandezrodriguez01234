'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import urllib3
import pandas as pd 

class WorldCovidDataCrawler(object):
    '''
    classdocs
    '''
    #Declaración de constantes
    URL_BASE = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
    URL_CONFIRMED = URL_BASE+ 'time_series_covid19_confirmed_global.csv'
    URL_DEATH = URL_BASE+'time_series_covid19_deaths_global.csv'
    URL_RECOVERED = URL_BASE+'time_series_covid19_recovered_global.csv'

    data_recuperados = None
    data_fallecidos = None
    data_confirmados = None
    
    def __init__(self):
        '''
        Constructor
        '''
    
    def extract(self):
        #Se obtienen los datos de recuperados, fallecidos y confirmados
        self.data_recuperados = pd.read_csv(WorldCovidDataCrawler.URL_RECOVERED) 
        self.data_fallecidos = pd.read_csv(WorldCovidDataCrawler.URL_DEATH) 
        self.data_confirmados = pd.read_csv(WorldCovidDataCrawler.URL_CONFIRMED) 
        
    def transform(self):
        self.data_recuperados['region'] = self.data_recuperados['Province/State'].fillna('')+self.data_recuperados['Country/Region'].fillna('')
        self.data_recuperados = self.data_recuperados.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long','region'], 
                                            var_name="fecha", 
                                            value_name="numero")
        self.data_recuperados['fecha'] = self.data_recuperados['fecha'].astype('datetime64[ns]')
        
        
        self.data_fallecidos['region'] = self.data_fallecidos['Province/State'].fillna('')+self.data_fallecidos['Country/Region'].fillna('')
        self.data_fallecidos = self.data_fallecidos.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long','region'], 
                                            var_name="fecha", 
                                            value_name="numero")
        self.data_fallecidos['fecha'] = self.data_fallecidos['fecha'].astype('datetime64[ns]')
        
        self.data_confirmados['region'] = self.data_confirmados['Province/State'].fillna('')+self.data_confirmados['Country/Region'].fillna('')
        self.data_confirmados = self.data_confirmados.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long','region'], 
                                            var_name="fecha", 
                                            value_name="numero")
        self.data_confirmados['fecha'] = self.data_confirmados['fecha'].astype('datetime64[ns]')
        
        
    def load(self):
        from common.SQLUtil import SQLUtil
        
        SQLUtil.pandasToTable(self.data_recuperados,'covid_19_recuperados')
        SQLUtil.pandasToTable(self.data_fallecidos,'covid_19_fallecidos')
        SQLUtil.pandasToTable(self.data_confirmados,'covid_19_confirmados')
        
    def exec(self):
        self.extract()
        self.transform()
        self.load()
