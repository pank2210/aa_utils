
import os
import pandas as pd
import datetime as dt
import re
import csv
import t_pattern as pattern

class CST:
    def __init__(self,ddir='../data/'):
        print("CST class initiated...")
        self.ddir = ddir
        self.o_fl = 'cst_out.csv'
        self.files = None
        self.fl_dict = {}
        self.files = []
    
    def parse_date_str( self, i_date):
        try:
            n_date = dt.datetime.strptime(i_date,'%m/%d/%y %H:%M').strftime('%Y%m%d%H%M')
        except ValueError:
            n_date = dt.datetime.strptime(i_date,'%Y/%m/%d %H:%M:%S').strftime('%Y%m%d%H%M')
        return n_date
        
    def load_files(self):
        print("CST::load_files called...")
        #self.files = files
        files = os.listdir(self.ddir)

        m_files = [fname for fname in files if re.match(r'.*?_search_query_.*?',fname)]
        self.search_query_cols =  ['USER_ELID', 'EVENT_DT', 'Search_Keyword']
        self.search_query_df = pd.DataFrame(columns=self.search_query_cols)
        for m_file in m_files:
            print("Processing search_query for [%s]" % (m_file))
            #self.files.append(m_file)
            temp_df = pd.read_csv(self.ddir + m_file)
            temp_df.columns = self.search_query_cols
            print("[%d] Record Loaded..." % (temp_df.EVENT_DT.count()))
            self.search_query_df = pd.concat( [self.search_query_df, temp_df], axis=0)
        self.search_query_df.EVENT_DT = self.search_query_df.EVENT_DT. \
                            apply(lambda x: self.parse_date_str(x))
        
        m_files = [fname for fname in files if re.match(r'.*?_doc_usage_.*?',fname)]
        self.doc_usage_cols =  ['USER_ELID', 'EVENT_DT', 'Document']
        self.doc_usage_df = pd.DataFrame(columns=self.doc_usage_cols)
        for m_file in m_files:
            print("Processing doc_usage for [%s]" % (m_file))
            #self.files.append(m_file)
            temp_df = pd.read_csv(self.ddir + m_file)
            temp_df.columns = self.doc_usage_cols
            print("[%d] Record Loaded..." % (temp_df.EVENT_DT.count()))
            self.doc_usage_df = pd.concat( [self.doc_usage_df, temp_df], axis=0)
        self.doc_usage_df.EVENT_DT = self.doc_usage_df.EVENT_DT. \
                            apply(lambda x: dt.datetime.strptime(x,'%m/%d/%y %I:%M %p').strftime('%Y%m%d%H%M'))
        
    def get_ground_truth(self):

        self.final_df = pd.merge( self.search_query_df, self.doc_usage_df, \
                                    how='inner', left_on=['USER_ELID','EVENT_DT'], right_on=['USER_ELID','EVENT_DT'])
        print(self.final_df.columns)
        #self.final_df = self.final_df.loc(,['USER_ELID','EVENT_DT','Document','Search_Keyword'])
        self.final_df['key'] = self.final_df.index
        #print(self.final_df.columns)
        self.final_df.to_csv(self.ddir + 'cst.csv')
        print("file[%s] count[%d]" % ('final_df',self.final_df.EVENT_DT.count()))

    def match_regex_pattern(self,val,patterns=[]):
        if len(patterns) == 0:
            return True

        if val:
            match_flag = "other"
            for pattern in patterns:
                m = re.search(pattern,val,re.I)
                if m:
                    #print("matched[%s] val[%s]" % (pattern,val))
                    match_flag = pattern
                    break
            return match_flag
        else:
            return "nullinput"


    def analyze_ground_truth(self):
        self.final_df = pd.read_csv( self.ddir + 'cst.csv')

        #self.final_df['m_pattern'] = 'none'
        self.final_df['doc_descr'] = ''
        self.final_df['doc_descr'] = self.final_df.Document.str.extract('^(.*?);.*?$')
        self.final_df['doc_id'] = ''
        self.final_df['doc_id'] = self.final_df.Document.str.extract('^.*?\/DOC-(\d+)\?.*?$')
        pat = pattern.Pattern(id='test1',default_match="other")
        #print(self.final_df.dtypes)
        self.final_df.doc_descr.fillna('NA',inplace=True)
        pat_df = pat.scan_df_for_pattern_match(sent_df=self.final_df)
        print("-------------------------------")
        #print(pat_df.head(10))
        self.final_df = pd.merge( self.final_df, pat_df, \
                                    how='inner', left_on=['key'], right_on=['key'])
        '''
        self.final_df['m_pattern'] = self.final_df.Document.apply(lambda x: \
                self.match_regex_pattern(x,['ptn','charg','recurr','coverage','signal','ptn','number','price plan']))
        print("After pattern match [%s]" % (self.final_df.columns))
        '''
        #print(self.final_df.sort_values(['doc_id'],ascending=True).head(5).doc_id)
        self.final_df.to_csv(self.ddir + 'final_df1.csv', index=False, sep='|')
        g_df = self.final_df[ \
                #(tcwa_eng_df.transferred == True) & \
                (self.final_df.m_pattern != 'other') \
                    ] \
                .groupby(['m_pattern','m_sub_pattern']) \
                ['EVENT_DT'].count() \
                .nlargest(50) \
                .reset_index(name='count') \
                .sort_values(['count'],ascending=False)
        #print(g_df.head())
        g_df.to_csv( self.ddir + 'g_pattern_match_count_' + self.o_fl, index=False)
        g_df = self.final_df[ \
                #(tcwa_eng_df.transferred == True) & \
                (self.final_df.m_pattern != 'other') \
                    ] \
                .groupby(['m_pattern','m_sub_pattern']) \
                ['doc_id'].nunique() \
                .nlargest(50) \
                .reset_index(name='nunique') \
                .sort_values(['m_pattern','nunique'],ascending=[False,False])
        #print(g_df.head())
        g_df.to_csv( self.ddir + 'g_pattern_match_doc_count_' + self.o_fl, index=False)
        print("Unique sub patterns count [%d]" % (self.final_df.USER_ELID.nunique()))
        agent_id_df = self.final_df.USER_ELID.unique()
        out = csv.writer(open(self.ddir + 'agent_id_' + self.o_fl,"w"),delimiter=',')
        out.writerow(agent_id_df)

        #Group by additional variables.
        g_df = self.final_df[ \
                #(tcwa_eng_df.transferred == True) & \
                (self.final_df.m_pattern != 'other') \
                    ] \
                .groupby(['m_pattern','m_sub_pattern','doc_id','doc_descr']) \
                ['EVENT_DT'].count() \
                .nlargest(1000) \
                .reset_index(name='count') \
                .sort_values(['m_pattern','m_sub_pattern','count'],ascending=[False,False,False])
        #print(g_df.head(25))
        g_df.to_csv( self.ddir + 'g_pattern_and_doc_' + self.o_fl, index=False)
        print("Unique Document count [%d]" % (self.final_df[self.final_df.m_pattern != 'other'].doc_id.nunique()))

    
if __name__ == "__main__":
    cst = CST(ddir='../data/')
    #cst.load_files()
    #cst.get_ground_truth()
    cst.analyze_ground_truth()


