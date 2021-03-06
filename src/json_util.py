import json
import os

import dpath.util 

from pprint import pprint

class JsonUtil:
  #json file handling utility class
  
  def __init__(self, jfilepath="../config/config.json",json_string=None):
    print("##json_util::init() file[{}]".format(jfilepath))
    
    #self.jfd = open('connection.json', 'r')
    if json_string != None:
      self.json_string = json_string
    else: 
      self.jfd = open(jfilepath,'r')
      self.json_string = json.load(self.jfd)
    #print("##json_string[{}]".format(type(self.json_string)))
    #print("##json_string[{}]".format(self.json_string.keys()))
  
  def getElementValue( self, elem_path):
     
    return dpath.util.values(self.json_string, elem_path)[0]
 
  def getElement(self, elem_path):
    print("##json_util::getElement elem_path[{}]".format(elem_path))
     
    #elems = elem_path.split('/')
     
    ''' 
    for x in dpath.util.search(self.json_string, elem_path, yielded=True): 
      print("******",type(x),x[1])
    print(dpath.util.values(self.json_string, '/train/img_dir_path')[0])
    ''' 
    #self.find( elems[-1], self.json_string)
    #print("elems [{}]".format(elems)) 
     
    return self.getElementValue(elem_path)
  
if __name__ == "__main__":
  ju = JsonUtil(jfilepath = './sample.json')
  print(ju.getElement(elem_path='/list[0]/id'))
  print(ju.getElement(elem_path='/list/contentID'))
