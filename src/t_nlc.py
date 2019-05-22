import json
from ibm_watson import AssistantV1
from ibm_watson import ApiException

import pandas as pd

import time_util as tu

api_cred = {
  "url": "https://gateway.watsonplatform.net/assistant/api",
  #Agent assist service related directory properties
  "workspace_id": "eb813b78-2c5f-4ac0-aedf-779e140fc463",
  "version": "2018-10-15",
  "username": "apikey",
  "password": "eiFjCV0ln3ECj4zma6Ji9OUveZZJ5dDxkPOr_Bpm3YS4"
}

class NLC:
  def log(self,fname,level=2,str=''):
    if level <= self.debug_level:
       print( "[%s::%s]|[%s]" %(self.cname,fname,str))
  
  def __init__(self,cred=None,ddir='../data/'):
    self.debug_level = 2
    self.ddir = ddir
    self.cname = 'NLC'
    fname = '__init__'
    #A timer keeper utility. 
    self.mytk = tu.MyTimeKeeper()
         
    self.log(fname,2,' call entry')
    self.o_fd = open( self.ddir + "assistant.out",'a')

    self.assistant = AssistantV1( \
            version = api_cred['version'], \
            username = api_cred['username'], \
            password = api_cred['password'], \
            url = api_cred['url'] \
         )
    '''
    self.session_id = self.assistant.create_session( \
                          assistant_id=api_cred['workspace_id'] \
                        ).get_result()
    '''

  def get_top_intent(self,resp_obj):
    fname = 'get_top_intent'
    ignore_intents = ['churn','chitchat']
    top_intent = None

    #self.log( fname, 2, 'call entry')
    intents = resp_obj['intents']

    for i,rec in enumerate(intents):
      '''
      print(rec['intent'])
      for k,v in rec.items():
        print("k[%s] v[%s]" % (k,v))
      '''
      if rec['intent'] in ignore_intents:
        continue
      else:
        top_intent = rec['intent']
        break
      
    return top_intent
      
  def invoke( self, i_text):
    fname = 'invoke'
    error_flag = False
    response = None
     
    #self.log(fname,2,' call entry')
    try:
      response = self.assistant.message( \
                    #session_id = self.session_id, \
                    workspace_id=api_cred['workspace_id'], \
                    input={ \
                        'text': i_text \
                    } \
                ).get_result()
    except ApiException as ex:
      self.log(fname,2,"API Failed for text[" + str(i_text) + "] status code [" + str(ex.code) + "] : message[" + str(ex.message) + "]")
      #self.log(fname,2,"API Failed for text[" + i_text + "] message[" + ex.message + "]")

    if response:
      resp_obj = json.dumps(response, indent=2)
      top_intent = self.get_top_intent(response)
    else:
      top_intent = 'INTENT_API_ERROR'

    #self.log(fname,1,top_intent)
    self.o_fd.write("%s|%s\n" % (i_text,top_intent))
    #self.o_fd.close()
    
    return top_intent

  def batch_invoke( self, i_file, key='id', value='subject', rec_del='|'):
    fname = 'batch_invoke'
     
    self.log(fname,2,' call entry')

    df = pd.read_csv( self.ddir + i_file, delimiter='|')
    df = df.head(50)
    #print(df.columns)

    cnt = 0
    skip_cnt = 500
    intents = []
    invoke_time = []
    self.mytk.get_time_passed('outer')
    key_dict = {}
    for i,rec in df.iterrows():
      '''
      if cnt < skip_cnt:
        cnt += 1
        continue
      '''
      #check if key already processed
      if rec[key] in key_dict:
        intents.append(key_dict[rec[key]])
        print("**duplicate rec [%s]" %(rec[key]))
        continue 
      else:
        self.mytk.get_time_passed('inner')
        intent = self.invoke(rec[value])
        invoke_time.append(self.mytk.get_time_passed('inner'))
        key_dict[rec[key]] = intent
        intents.append(intent)
        cnt += 1
        if cnt % 100 == 0:
          print("processed[%d] avg_time[%.2f]" % (cnt,(sum(invoke_time)/cnt)))

    df['intent'] = intents
    o_file = self.ddir + "u_" + i_file
    df.to_csv( o_file, sep='|', index=False)
    self.log(fname,2,"processed[%d] in [%d]ms with output file[%s]" % (cnt,self.mytk.get_time_passed('outer'),o_file))

  def print_intent_spread(self,i_file,g_count_key='contentID'):
    fname = 'print_intent_spread'
     
    self.log(fname,2,' call entry')

    df = pd.read_csv( self.ddir + i_file, delimiter='|')
    df.intent = df.intent.fillna('NA')
    g_df = df[ \
        #(df.status_code != 200) & \
        (df[g_count_key] > 0) \
            ] \
        .groupby(['intent']) \
        [g_count_key].count() \
        .nlargest(1000) \
        .reset_index(name='count') \
        .sort_values(['count'],ascending=False)
    print(g_df)

if __name__ == '__main__':
  print('NLC/Assistant Tester')
  text = "I want to change my phone number"
  i_file = 'space_all_df.csv'
  i_file = 'ner_processed_graph_df.csv'
  nlc = NLC()
  #nlc.invoke(text)
  #nlc.batch_invoke('space_all_df.csv',)
  #nlc.print_intent_spread(i_file=i_file,g_count_key='contentID')
  nlc.batch_invoke( i_file=i_file, key='sent_id', value='text', rec_del='|')
  nlc.print_intent_spread( i_file= 'u_' + i_file, g_count_key='sent_id')
