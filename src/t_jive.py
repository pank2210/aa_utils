
import requests
from requests.auth import HTTPBasicAuth

import json
import pandas as pd
import csv

"""from requests.utils import DEFAULT_CA_BUNDLE_PATH; print(DEFAULT_CA_BUNDLE_PATH)"""

class JiveCrawler: 
  def __init__(self,p_doc_id,c_doc_id_samples=None):  
    print("JiveCrawler initiated for p_doc_id[%s]" % (p_doc_id))
    self.verify = "C:\\Program Files\\Python36\\Lib\\site-packages\\certifi\\cacert.pem" 
    self.c_url = 'https://iconnect-test.sprint.com/api/core/v3/contents/?filter=entityDescriptor(102,%s)'
    #p_url = 'https://iconnect-test.sprint.com/api/core/v3/places/%s'
    self.p_url = 'https://iconnect-test.sprint.com/api/core/v3/places/%s/contents'
    self.userid = '$ssagentassist'
    self.pwd = 'Ag3nt_A55ist'
    self.p_doc_id = p_doc_id  # use 1414 for Account Management - Care
    self.output_df_fl = 'space_' + "_".join(p_doc_id) + '_df.csv'
    self.err_df_fl = 'space_err_df.csv'
     
    #col_names = ['id','name','subject','type','contentID','html_ref','Parent']
    self.col_names = ['id','name','subject','type','contentID','html_ref']
    self.err_col_names = ['space_id','http_response_code','url']
    self.df = pd.DataFrame(columns=self.col_names)
    self.df_dict = {}
    self.err_df = pd.DataFrame(columns=self.err_col_names)
    self.err_df_dict = {}

    if c_doc_id_samples == None:
      self.c_doc_id_samples = []
    else:
      self.c_doc_id_samples = c_doc_id_samples
    self.sample_size = .1
    self.random_seed = 1001
    
    for col in self.col_names: 
      self.df_dict[col] = []

    for col in self.err_col_names: 
      self.err_df_dict[col] = []
   
  def get_response(self,req_url): 
    jdoc = None
    resp = requests.get( req_url, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
    if resp.status_code == 200:
       print("Response received.")
       jdoc = json.loads(resp.text)
       
    else:
       print("Response to request url[%s] failed with status_code[%d]" % (req_url,resp.status_code))
       print(resp.headers)
    
    return resp.status_code, jdoc
  
  def get_child_contents(self,jdoc):
    #jdoc = json.loads(resp.text)
    #list/resources/parentPlace
    print("*****Data collected so far [%d]" % (len(self.df_dict[self.col_names[0]])))
    
    #iterate child
    for item in jdoc['list']:
      my_dict = {}
      #print(item['id'],item['subject'])
      
      my_dict[self.col_names[0]] = item['id']
      my_dict[self.col_names[1]] = item['parentPlace']['name']
      my_dict[self.col_names[2]] = item['subject']
      my_dict[self.col_names[3]] = item['type']
      my_dict[self.col_names[4]] = item['contentID']
      my_dict[self.col_names[5]] = item['resources']['html']['ref']
      #my_dict[self.col_names[6]] = item['parent']
      
      for col in self.col_names:
        arr = self.df_dict[col]
        arr.append(my_dict[col])
        self.df_dict[col] = arr
      
  def crawl(self,next_link=True): 
    p_doc_id = self.p_doc_id

    if type(p_doc_id) is list:
      for doc_id in p_doc_id:
        self.crawl_url(doc_id,next_link)
    else:
        self.crawl(p_doc_id,next_link)

  def crawl_url(self,doc_id,next_link=True): 
    url = self.p_url

    req_url = url % (doc_id)     
    #print("Parent doc_id[%s], connecting to url[%s] for crawl..." % (doc_id,req_url)) 
    print("Crawling parent doc_id[%s]..." % (doc_id)) 
    status_code,jdoc = self.get_response(req_url)
    if status_code != 200:
      print("http get failed with status_code[%d]. for doc_id[%s]." % (status_code,doc_id))
      err_dict = {}
      #print(item['id'],item['subject'])
      
      err_dict[self.err_col_names[0]] = doc_id
      err_dict[self.err_col_names[1]] = status_code
      err_dict[self.err_col_names[2]] = req_url
      
      for col in self.err_col_names:
        arr = self.err_df_dict[col]
        arr.append(err_dict[col])
        self.err_df_dict[col] = arr
      #exit(-10)
    else:
      self.get_child_contents(jdoc)
      #next_link = True
      if 'links' in jdoc:
        while next_link:
          if 'next' not in jdoc['links']:
            print("Traversing stoped as next link is absent")
            next_link = False
          else:
            req_url = jdoc['links']['next']
            print("Traversing next link [%s]" % (req_url))
            status_code,jdoc = self.get_response(req_url)
            self.get_child_contents(jdoc)
            if status_code != 200:
              print("http get for [%s] failed with status_code[%d]. Exiting untill error is fixed." % (req_url,status_code))
              next_link = False
      else:
        print("No Traversing required as links tag is absent")
        
  def generate_output(self):
    #build output dataframe
    for col in self.err_col_names:
      self.err_df[col] = self.err_df_dict[col]
    self.err_df.to_csv(self.err_df_fl,sep='|',index=False)
    for col in self.col_names:
      self.df[col] = self.df_dict[col]
    self.df.to_csv(self.output_df_fl,sep='|',index=False)
    df_ids = self.df.id.unique()
    out = csv.writer(open('id_' + self.output_df_fl,"w"),delimiter=',')
    out.writerow(df_ids)

    sample_df = self.df.sample(frac=self.sample_size,random_state=self.random_seed)
    if len(self.c_doc_id_samples) > 0:
      #print(sample_df.dtypes)
      #print("already in sample",sample_df.id.unique())
      #print("c_doc_id_samples",self.c_doc_id_samples)
      samples_already_in = sample_df[sample_df.id.isin(self.c_doc_id_samples)].id.unique()
      print("samples_already_in",samples_already_in)
      samples_doc_id = list(set(self.c_doc_id_samples)-set(samples_already_in))
      print("samples_doc_id",samples_doc_id)
      sample_doc_df = self.df[self.df.id.isin(samples_doc_id)]
      print(sample_doc_df.head(5))
      sample_df = sample_df.append(sample_doc_df)
      print(sample_df.head(5))
    sample_df.to_csv('sample_' + self.output_df_fl,sep='|',index=False)
    sample_ids = sample_df.id.unique()
    out = csv.writer(open('sample_id_' + self.output_df_fl,"w"),delimiter=',')
    out.writerow(sample_ids)
    #print(self.df.head())
    
  #fd = open("sample.json","w")
  #fd.write(resp.text)
  #jdata.dump(jdata,fd, indent=4)
  
#print(data["list"] [0]["content"] ["text"])
if __name__ == "__main__":
   print("Main started....")
   jc = JiveCrawler(p_doc_id=['2049','2050','2332','2499'],c_doc_id_samples=['16297','11208','11209','11210','9935','19399'])
   jc.crawl(next_link=False)
   #jc.crawl()
   jc.generate_output()
