'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s' , level=logging.DEBUG)
    
    logging.info('Inicio del proceso de extracción de los datos médicos')
    from crawlers.medical.WorldCovidDataCrawler import WorldCovidDataCrawler
    worldCovidDataCrawler = WorldCovidDataCrawler()
    worldCovidDataCrawler.exec()
    logging.info('FIN del proceso de extracción de los datos médicos')
     
    logging.info('Inicio del proceso de extracción de los datos de tráfico de Madrid calle30')
    from crawlers.social.MadridCalle30Crawler import MadridCalle30Crawler
    madridCalle30Crawler = MadridCalle30Crawler()
    madridCalle30Crawler.exec()
    logging.info('FIN del proceso de extracción de los datos de tráfico de Madrid calle30')
     
    logging.info('Inicio del proceso de extracción del registro de defunciones del MOMO')
    from crawlers.social.RegistroDefuncionesCrawler import RegistroDefuncionesCrawler
    registroDefuncionesCrawler = RegistroDefuncionesCrawler()
    registroDefuncionesCrawler.exec()
    logging.info('FIN del proceso de extracción del registro de defunciones del MOMO')
      
    logging.info('Inicio del proceso de extracción de los valores del ibex35')
    from crawlers.economic.IBEX35DataCrawler import IBEX35DataCrawler
    IBEX35DataCrawler = IBEX35DataCrawler()
    IBEX35DataCrawler.exec()
    logging.info('FIN del proceso de extracción de los valores del ibex35')
    
    logging.info('Inicio del proceso de extracción de los valores de los datos del INE')
    from crawlers.economic.INEDataCrawler import INEDataCrawler
    INEDataCrawler = INEDataCrawler()
    INEDataCrawler.exec()
    logging.info('FIN del proceso de extracción de los valores de los datos del INE')
