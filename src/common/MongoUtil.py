'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''
import pandas as pd 
from pymongo import MongoClient

class MongoLoad(object):
    '''
    classdocs
    '''
    MONGO_HOST="localhost"
    MONGO_PORT= 27017
    
    CONNECTION_STRING="mongodb://"+ MONGO_HOST +":"+ str(MONGO_PORT)
    
    mongoDb = MongoClient(CONNECTION_STRING).TFM_Jfernandez
    
    
    def __init__(self, params):
        '''
        Constructor
        '''

    @staticmethod 
    def insertMany(dataCollection, collection_name, truncate):
        if truncate:
            MongoLoad.mongoDb[collection_name].drop()
        collection = MongoLoad.mongoDb[collection_name]
        collection.insert_many(dataCollection)
