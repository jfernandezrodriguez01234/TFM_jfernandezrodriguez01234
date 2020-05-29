'''
Created on 25 abr. 2020

@author: jesus.fernandez
'''

from cv2 import cv2
import numpy as np
import glob
import urllib.request

base_url = "http://www.euromomo.eu/slices/"
path =  "Maps-{}/"
filename =  "MAP-{}-{:02d}.png"
anhos = list(range(2018,2021))
semanas = list(range(1,55))

for anho in anhos:
    ruta = path.format(anho)
    for semana in semanas:
        try:
            fichero = filename.format(anho,semana)
            print("Se recuperan los datos del año {} semana {} en el fichero {}".format(anho,semana,fichero))
            print("URL: {}".format(base_url+ruta+fichero, "./data/"+fichero))
            urllib.request.urlretrieve(base_url+ruta+fichero, "./data/"+fichero)
        except:
            break
        
img_array = []
for filename in glob.glob('./data/*.jpg'):
    img = cv2.imread(filename)
    height, width, layers = img.shape
    size = (width,height)
    img_array.append(img)
 
 
out = cv2.VideoWriter('project.avi',cv2.VideoWriter_fourcc(*'DIVX'), 15, size)
 
for i in range(len(img_array)):
    out.write(img_array[i])
out.release()
