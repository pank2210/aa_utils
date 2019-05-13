
import requests
from requests.auth import HTTPBasicAuth

import re
import json
import pandas as pd
import csv

import time_util as tu

"""from requests.utils import DEFAULT_CA_BUNDLE_PATH; print(DEFAULT_CA_BUNDLE_PATH)"""

class JiveCrawler:
  
  #Main contruxtor for JiveCrawler Ideally has predefined defaults configured for developer.
  def __init__( self,
                p_doc_id=[], \
                c_doc_id_samples=None, \
                j_userid='$ssagentassist', \
                j_pwd = 'Ag3nt_A55ist', \
                target_env='iconnect-test', \
                ssl_cacert_path="C:\\Program Files\\Python36\\Lib\\site-packages\\certifi\\cacert.pem", \
                ddir='../data/' \
                ):  
    print("JiveCrawler initiated for p_doc_id[%s]" % (p_doc_id))
    self.ddir = ddir
    self.target_env = target_env

    #SSL Cert path. Changes as per local installation
    self.verify =  ssl_cacert_path

    #context URL's required for Jive API calls.
    self.c_url = 'https://%s.sprint.com/api/core/v3/contents/?filter=entityDescriptor(102,%s)'
    self.s_url = 'https://%s.sprint.com/api/core/v3/places/%s'
    self.p_url = 'https://%s.sprint.com/api/core/v3/places/%s/contents'

    #Jive API access credentials. Normally user_id is service id made available from Jive
    self.userid = j_userid
    #self.pwd = j_pwd
    self.pwd = 'A55ist!a6ent'

    #Preconfigure space_id used for pulling all child content. Has to be overrided by setter.
    self.p_doc_id = p_doc_id  # use 1414 for Account Management - Care

    #Static output files for wrinting results.
    self.output_df_fl = 'space_all_df.csv'
    #col_names = ['id','name','subject','type','contentID','html_ref','Parent']
    self.col_names = ['id','name','subject','type','contentID','html_ref']
    self.df = pd.DataFrame(columns=self.col_names)
    self.df_dict = {}
    for col in self.col_names: 
      self.df_dict[col] = []

    #A timer keeper utility. 
    self.mytk = tu.MyTimeKeeper()
     
    #Initial parameter required for Error details collection and printing.
    self.err_col_names = ['space_id','http_response_code','url']
    self.err_df_fl = 'space_err_df.csv'
    self.err_df = pd.DataFrame(columns=self.err_col_names)
    self.err_df_dict = {}
    for col in self.err_col_names: 
      self.err_df_dict[col] = []

    #Additional param kept for testing only where instead of pulling all Jive content only key documents and noise can be used.
    if c_doc_id_samples == None:
      self.c_doc_id_samples = []
    else:
      self.c_doc_id_samples = c_doc_id_samples
    self.sample_size = .1
    self.random_seed = 1001
    
    #get place id for document urls
    self.output_df_fl1 = 'space_place_all_df_.csv'
    self.err_df_fl1 = 'space_place_err_df.csv'
    self.df_dict1 = {}
    self.err_df_dict1 = {}
    self.col_names1=['spaceId','placeId','name','uri']
    self.err_col_names1=['docId','http_response_code','uri']
    self.err_df1 = pd.DataFrame(columns=self.err_col_names1)
    self.df1 = pd.DataFrame(columns=self.col_names1)

  def set_p_doc_id(self,p_doc_id):  
    self.p_doc_id = p_doc_id  # use ['1414'] for Account Management - Care

  def get_response(self,req_url): 
    jdoc = None
    resp = requests.get( req_url, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
    if resp.status_code == 200:
       #print("Response received.")
       jdoc = json.loads(resp.text)
       
    else:
       print("Response to request url[%s] failed with status_code[%d]" % (req_url,resp.status_code))
       #print(resp.headers)
    
    return resp.status_code, jdoc

  def get_place_id(self,space_id): 
    jdoc = None
    req_url='https://' + self.target_env + '.sprint.com/api/core/v3/places/?filter=entityDescriptor(14,'+space_id+')'
    resp = requests.get( req_url, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
    if resp.status_code == 200:
       #print("Response received.")
       jdoc = json.loads(resp.text)
       #print(jdoc['list'][0]['placeID'])
    else:
       print("Response to request url[%s] failed with status_code[%d]" % (req_url,resp.status_code))
       #print(resp.headers)
    
    return jdoc['list'][0]['placeID']

  def get_docids_cst_extract(self):
    with open('final_df1.csv') as csv_file:
      csv_reader = csv.load_csv(csv_file, delimiter=',')
      line_count = 0
      #doc_id_set = set()
      thislist = []
      for row in csv_reader:
          if line_count == 0:
              print(f'Column names are {", ".join(row)}')
              line_count += 1
          else:
              doc_url=row[5]
              doc_id=doc_url[37:-8]
              if row[9] != 'other':
                thislist.append(doc_id)
                print('its expected doc:'+doc_id)
              else:
                print('its other doc :'+doc_id)

              line_count += 1
 
      s = set(thislist)
      print(s)

      return s

  def check_access_to_doc_ids(self,i_file): 
    jdoc = None
    #doc_ids=self.get_doc_ids()
    space_ids = []
    status_codes = []
    status_codes_dict = {}
    _not_in_patterns = {
        'bst': r'BST',
        'spp': r'SPP',
        'vmu': r'VMU'
    }

    print("Processing check_access_to_doc_ids...")
    df = pd.read_csv( self.ddir + i_file)
    cols = ['m_pattern','m_sub_pattern','doc_id','doc_descr','usage','train_flag']
    #print(df.columns)
    df.columns = cols
    print("Total docs [%d]" % (df.doc_id.count()))
    print("Trainable Doc's [%d]" % (df[df.train_flag.isin(['Y','y'])].doc_id.count()))
    df = df[df.train_flag.isin(['Y','y'])]
    print("Total docs with train Flags [%d]" % (df.doc_id.count()))
    #df = df[df.m_pattern != 'other'].head(100)
    #print(df.doc_id.head())
    
    cnt = 0
    jive_calls = 0
    jive_calls_failed = 0
    invoke_time = []
    for i,rec in df.iterrows():
      not_in_flag = False
      for negate_pattern in _not_in_patterns:
        m = re.search( negate_pattern, rec.doc_descr, re.I)
        #print("****s_key[%s] sent[%s] m[%s] pattern[%s]" % (s_key,sent,m,negate_pattern))
        if m:
            #even if one not_in_pattern match do not continue processing
            not_in_flag = True
            break
      
      if not_in_flag:
        #if not_in_pattern match then do no further processing.
        #space_ids.append('00000')
        status_codes.append(999)
      else:
        if rec.doc_id in status_codes_dict:
          val = status_codes_dict[rec.doc_id]
          #space_ids.append(val[0])
          status_codes.append(val[1])
        else:
          #req_url = self.c_url % (rec.doc_id)
          #print("getting doc_id[%s]" % (rec.doc_id))
          req_url1 = 'https://%s.sprint.com/api/core/v3/contents/?filter=entityDescriptor(102,%d)' % ( self.target_env, rec.doc_id)
          #print(req_url1)
          self.mytk.get_time_passed('inner')
          resp = requests.get( req_url1, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
          invoke_time.append(self.mytk.get_time_passed('inner'))
          status_codes.append( resp.status_code)
          jive_calls += 1
          if resp.status_code != 200:
            jive_calls_failed += 1
          #space_ids.append('00000')
          status_codes_dict[rec.doc_id] = [rec.doc_id,status_codes[-1]]
      if cnt % 100 == 0:
        print("processed[%d] jive_calls[%d] jive_calls_failed[%d] avg_time[%.2f]" % (cnt,jive_calls,jive_calls_failed,(sum(invoke_time)/jive_calls)))
      cnt += 1
      	
    #df['space_id'] = space_ids
    df['status_code'] = status_codes
    print('Processed docs ',df.doc_id.count(),' status_codes count ',len(status_codes),' Jive calls ',jive_calls)

    print("[%d] rec's processed..." % (cnt))
    print("Status Codes [%s]" % (set(status_codes)))
    df.to_csv( self.ddir + 'p_doc_access_check_df.csv', sep='|', index=False)
    df[(df.status_code == 200)].to_csv( self.ddir + 'p_doc_access_check_200_df.csv', sep='|', index=False)
    df[(df.status_code != 200) & (df.status_code != 999)].to_csv( self.ddir + 'p_doc_access_check_error_df.csv', sep='|', index=False)
    g_df = df[ \
            #(df.status_code != 200) & \
            (df.m_pattern != 'other') \
                ] \
            .groupby(['status_code']) \
            ['doc_id'].nunique() \
            .nlargest(1000) \
            .reset_index(name='nunique') \
            .sort_values(['nunique'],ascending=False)
    print(g_df.head())
    g_df.to_csv( self.ddir + 'g_access_check_sc.csv', sep='|', index=False)

    return df[df.status_code != 200]

  def get_placeids_from_spaceids(self,i_file='a9_jive_space_id_access_list_20190502.csv'): 
    jdoc = None
    spaceids_url = self.s_url #'https://%s.sprint.com/api/core/v3/places/%d'
    placeids_url = self.p_url #'https://%s.sprint.com/api/core/v3/places/%d/contents'
    #doc_ids=self.get_doc_ids()
    space_ids = []
    place_ids = []
    parent_urls = []
    status_codes = []
    parent_urls_dict = {}
    status_codes_dict = {}
    placeids_dict = {}
    df = None

    print("Processing spaceids from [%s]..." % (i_file))
    df = pd.read_csv( self.ddir + i_file,encoding="ISO-8859-1")
    #df = df[(df.space_id == 2007) | (df.space_id == 2008)]
    print(df.head())
    
    cnt = 0
    jive_calls = 0
    jive_calls_failed = 0
    for i,rec in df.iterrows():
      if rec.space_access != 'y':
        status_codes.append(999)
        parent_urls.append('NA')
        continue
      else:
        req_url1 = spaceids_url % (self.target_env,rec.space_id)
        resp = requests.get( req_url1, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
        status_codes.append( resp.status_code)
        jive_calls += 1
        if resp.status_code == 200:
            #print("Response header. [%s]" % (resp.headers))
            #print("Response text. [%s]" % (resp.text))
            jdoc = json.loads(resp.text)
            #print(jdoc['list'][0]['parentPlace'])
            parent_url = jdoc['parent']
            #print("****",item['uri'][52:])
            parent_urls.append(parent_url)
        else:
            jive_calls_failed += 1
            parent_urls.append('ERROR')
            #print("Response to request url[%s] failed with status_code[%d]" % (rec.doc_id,resp.status_code))
            #print(resp.headers)
            #space_ids.append('00000')
            #status_codes_dict[rec.doc_id] = [space_ids[-1],status_codes[-1]]
      if cnt % 1000 == 0:
        print("processed[%d] jive_calls[%d] jive_calls_failed[%d]" % (cnt,jive_calls,jive_calls_failed))
      cnt += 1
      	
    #df['space_id'] = space_ids
    df['status_code'] = status_codes
    df['parent_url'] = parent_urls

    print("[%d] rec's processed..." % (cnt))
    print("Status Codes [%s]" % (set(status_codes)))
    df.to_csv( self.ddir + 'a9_space_ids_upd.csv', sep=',', index=False)
    g_df = df[ \
            #(df.status_code != 200) & \
            (df.space_access == 'y') \
                ] \
            .groupby(['status_code']) \
            ['space_id'].count() \
            .nlargest(1000) \
            .reset_index(name='count') \
            .sort_values(['count'],ascending=False)
    print(g_df)
    g_df.to_csv( self.ddir + 'a9_g_space_ids_sc.csv', sep='|', index=False)

    return (set(space_ids)-{'00000'})

  def process_spaces_with_appids(self,i_file='a9_jive_space_id_access_list_20190512.csv'): 
    jdoc = None
    spaceids_url = self.s_url #'https://%s.sprint.com/api/core/v3/places/%d'
    placeids_url = self.p_url #'https://%s.sprint.com/api/core/v3/places/%d/contents'
    space_ids = []
    place_ids = []
    parent_urls = []
    status_codes = []
    parent_urls_dict = {}
    status_codes_dict = {}
    placeids_dict = {}
    df = None

    print("Processing spaceids from [%s]..." % (i_file))
    df = pd.read_csv( self.ddir + i_file,encoding="ISO-8859-1")
    #df = df[(df.space_id == 2007) | (df.space_id == 2008)]
    print(df.head())
    
    cnt = 0
    jive_calls = 0
    jive_calls_failed = 0
    for i,rec in df.iterrows():
      if rec.space_access != 'y':
        status_codes.append(999)
        parent_urls.append('NA')
        continue
      else:
        req_url1 = spaceids_url % ( self.target_env, rec.space_appid)
        resp = requests.get( req_url1, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
        status_codes.append( resp.status_code)
        jive_calls += 1
        if resp.status_code == 200:
            #print("Response header. [%s]" % (resp.headers))
            #print("Response text. [%s]" % (resp.text))
            jdoc = json.loads(resp.text)
            #print(jdoc['list'][0]['parentPlace'])
            if 'parent' in jdoc:
                parent_url = jdoc['parent']
            else:
                parent_url = 'not found'
            #print("****",item['uri'][52:])
            parent_urls.append(parent_url)
            space_ids.append(rec.space_appid)
        else:
            jive_calls_failed += 1
            parent_urls.append('ERROR')
            #print("Response to request url[%s] failed with status_code[%d]" % (rec.doc_id,resp.status_code))
            #print(resp.headers)
            #space_ids.append('00000')
            #status_codes_dict[rec.doc_id] = [space_ids[-1],status_codes[-1]]
      if cnt % 1000 == 0:
        print("processed[%d] jive_calls[%d] jive_calls_failed[%d]" % (cnt,jive_calls,jive_calls_failed))
      cnt += 1
      	
    #df['space_id'] = space_ids
    df['status_code'] = status_codes
    df['parent_url'] = parent_urls

    print("[%d] rec's processed..." % (cnt))
    print("[%d] space id's tested..." % (len(space_ids)))
    print("Status Codes [%s]" % (set(status_codes)))
    df.to_csv( self.ddir + 'a9_space_ids_upd.csv', sep=',', index=False)
    g_df = df[ \
            #(df.status_code != 200) & \
            (df.space_access == 'y') \
                ] \
            .groupby(['status_code']) \
            ['space_id'].count() \
            .nlargest(1000) \
            .reset_index(name='count') \
            .sort_values(['count'],ascending=False)
    print(g_df)
    g_df.to_csv( self.ddir + 'a9_g_space_ids_sc.csv', sep='|', index=False)

    return (set(space_ids))

  def get_parent_ids(self,i_file='final_df1.csv',i_df=None): 
    jdoc = None
    #doc_ids=self.get_doc_ids()
    space_ids = []
    status_codes = []
    status_codes_dict = {}
    df = None

    if i_df:
      print("Processing get_parent_ids from input dataframe param...")
      df = i_df
      #drop status_code as input will have it and output needs to generate one
      df.drop('status_code',axis=1,inplace=True)
    else:
      print("Processing get_parent_ids from [%s]..." % (i_file))
      df = pd.read_csv( self.ddir + i_file, delimiter='|')
    #df = df[df.m_pattern != 'other'].head(100)
    #print(df.doc_id.head())
    
    cnt = 0
    jive_calls = 0
    jive_calls_failed = 0
    for i,rec in df.iterrows():
      if rec.m_pattern == 'other':
        space_ids.append('00000')
        status_codes.append(999)
      else:
        if rec.doc_id in status_codes_dict:
          val = status_codes_dict[rec.doc_id]
          space_ids.append(val[0])
          status_codes.append(val[1])
        else:
          #req_url = self.c_url % (rec.doc_id)
          #print("getting doc_id[%s]" % (rec.doc_id))
          req_url1 = self.c_url % (self.target_env,rec.doc_id)
          #print(req_url1)
          resp = requests.get( req_url1, auth=HTTPBasicAuth( self.userid, self.pwd), verify=self.verify)
          status_codes.append( resp.status_code)
          jive_calls += 1
          if resp.status_code == 200:
            #print("Response header. [%s]" % (resp.headers))
            #print("Response text. [%s]" % (resp.text))
            jdoc = json.loads(resp.text)
            #print(jdoc['list'][0]['parentPlace'])
            item=jdoc['list'][0]['parentPlace']
            #print("****",item['uri'][52:])
            space_ids.append(item['uri'][52:])
          else:
            jive_calls_failed += 1
            #print("Response to request url[%s] failed with status_code[%d]" % (rec.doc_id,resp.status_code))
            #print(resp.headers)
            space_ids.append('00000')
          status_codes_dict[rec.doc_id] = [space_ids[-1],status_codes[-1]]
      if cnt % 1000 == 0:
        print("processed[%d] jive_calls[%d] jive_calls_failed[%d]" % (cnt,jive_calls,jive_calls_failed))
      cnt += 1
      	
    df['space_id'] = space_ids
    df['status_code'] = status_codes
    print(df.doc_id.count(),len(space_ids),len(status_codes))

    print("[%d] rec's processed..." % (cnt))
    print("Status Codes [%s]" % (set(status_codes)))
    df.to_csv( self.ddir + 'p_space_ids_df.csv', sep='|', index=False)
    g_df = df[ \
            #(df.status_code != 200) & \
            (df.m_pattern != 'other') \
                ] \
            .groupby(['status_code','m_pattern','m_sub_pattern']) \
            ['doc_id'].nunique() \
            .nlargest(1000) \
            .reset_index(name='nunique') \
            .sort_values(['nunique'],ascending=False)
    #print(g_df.head())
    g_df.to_csv( self.ddir + 'g_parent_sc.csv', sep='|', index=False)
    g_df = df[ \
            (df.status_code == 200) & \
            (df.m_pattern != 'other') \
                ] \
            .groupby(['space_id','m_pattern','m_sub_pattern']) \
            ['doc_id'].nunique() \
            .nlargest(100) \
            .reset_index(name='nunique') \
            .sort_values(['nunique'],ascending=False)
    #print(g_df.head())
    g_df.to_csv( self.ddir + 'g_space_ids_df.csv', sep='|', index=False)

    return (set(space_ids)-{'00000'})
    

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
      
  def crawl_place_id(self,space_list): 
    #p_doc_id = list
    place_list=[]
    if type(space_list) is list:
      for space_id in space_list:
        place_list.append(self.get_place_id(space_id))
    else:
        self.get_place_id('1000')
    #print(c_list)    
    return place_list

  def crawl(self,next_link=True): 
    p_doc_id = self.p_doc_id

    if type(p_doc_id) is list:
      for doc_id in p_doc_id:
        try:
          self.crawl_url(doc_id,next_link)
        except:
          pass
    else:
        self.crawl(p_doc_id,next_link)

  def crawl_url(self,doc_id,next_link=True): 
    url = self.p_url

    req_url = url % (self.target_env,doc_id)     
    #print("Parent doc_id[%s], connecting to url[%s] for crawl..." % (doc_id,req_url)) 
    #print("Crawling parent doc_id[%s]..." % (doc_id)) 
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
            #print("Traversing stoped as next link is absent")
            next_link = False
          else:
            req_url = jdoc['links']['next']
            #print("Traversing next link [%s]" % (req_url))
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
    self.err_df.to_csv(self.ddir + self.err_df_fl,sep='|',index=False)
    for col in self.col_names:
      self.df[col] = self.df_dict[col]
    self.df.to_csv(self.ddir + self.output_df_fl,sep='|',index=False)
    df_ids = self.df.id.unique()
    out = csv.writer(open(self.ddir + 'id_' + self.output_df_fl,"w"),delimiter=',')
    out.writerow(df_ids)

    sample_df = self.df.sample(frac=self.sample_size,random_state=self.random_seed)
    if len(self.c_doc_id_samples) > 0:
      #print(sample_df.dtypes)
      #print("already in sample",sample_df.id.unique())
      #print("c_doc_id_samples",self.c_doc_id_samples)
      samples_already_in = sample_df[sample_df.id.isin(self.c_doc_id_samples)].id.unique()
      #print("samples_already_in",samples_already_in)
      samples_doc_id = list(set(self.c_doc_id_samples)-set(samples_already_in))
      #print("samples_doc_id",samples_doc_id)
      sample_doc_df = self.df[self.df.id.isin(samples_doc_id)]
      #print(sample_doc_df.head(5))
      sample_df = sample_df.append(sample_doc_df)
      #print(sample_df.head(5))
    sample_df.to_csv(self.ddir + 'sample_' + self.output_df_fl,sep='|',index=False)
    sample_ids = sample_df.id.unique()
    out = csv.writer(open(self.ddir + 'sample_id_' + self.output_df_fl,"w"),delimiter=',')
    out.writerow(sample_ids)
    #print(self.df.head())
    
  #fd = open("sample.json","w")
  #fd.write(resp.text)
  #jdata.dump(jdata,fd, indent=4)
  
#print(data["list"] [0]["content"] ["text"])
if __name__ == "__main__":
   print("Main started....")

   jc = JiveCrawler(target_env='iconnect')
   #jc.get_placeids_from_spaceids()
   placeSet = jc.process_spaces_with_appids()
   #print(placeSet)
   #doc_df = jc.check_access_to_doc_ids('aa_jive_doc_list_review_phase1_0421.csv')
   #placeSet=jc.get_parent_ids()
   #jc.set_p_doc_id(p_doc_id=['4781','13317','13404'])
   jc.set_p_doc_id(p_doc_id=list(placeSet))
   jc.crawl(next_link=True)
   jc.generate_output()
