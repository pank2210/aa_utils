
import os
import pandas as pd
import datetime as dt
import re
import csv

class CST:
    def __init__(self,ddir='../data/'):
        print("CST class initiated...")
        self.ddir = ddir
        self.o_fl = 'cst_out.csv'
        self.files = None
        self.fl_dict = {}
        self.files = []
    
    def load_files(self,files):
        print("CST::load_files called...")
        #self.files = files

        for fl in files:
            print("processing [%s]" % (fl))
            self.files.append(fl)
            self.fl_dict[fl] = pd.read_csv(self.ddir + fl)

            #print(self.fl_dict[fl].head())
        
        cst.parse_search_query()
        cst.parse_doc_usage()

    def parse_search_query(self):
        cols = ['USER_ELID', 'EVENT_DT', 'Search_Keyword']
        self.fl_dict[self.files[1]].columns = cols
        print(self.fl_dict[self.files[1]].columns)
        # 2/28/19 0:00
        self.fl_dict[self.files[1]].EVENT_DT = self.fl_dict[self.files[1]].EVENT_DT. \
                            apply(lambda x: dt.datetime.strptime(x,'%m/%d/%y %H:%M').strftime('%Y%m%d%H%M'))
        print(self.fl_dict[self.files[1]].EVENT_DT.head())

    def parse_doc_usage(self):
        cols = ['USER_ELID', 'EVENT_DT', 'Document']
        #self.fl_dict[self.files[1]].columns = cols
        print(self.fl_dict[self.files[0]].columns)
        # 2/28/19 0:00
        self.fl_dict[self.files[0]].EVENT_DT = self.fl_dict[self.files[0]].EVENT_DT. \
                            apply(lambda x: dt.datetime.strptime(x,'%m/%d/%y %I:%M %p').strftime('%Y%m%d%H%M'))
        print(self.fl_dict[self.files[0]].EVENT_DT.head())

    def get_ground_truth(self):
        print("file[%s] count[%d]" % (self.files[0],self.fl_dict[self.files[0]].EVENT_DT.count()))
        print("file[%s] count[%d]" % (self.files[1],self.fl_dict[self.files[1]].EVENT_DT.count()))

        self.final_df = pd.merge( self.fl_dict[self.files[0]], self.fl_dict[self.files[1]], \
                                    how='inner', left_on=['USER_ELID','EVENT_DT'], right_on=['USER_ELID','EVENT_DT'])
        print(self.final_df.columns)
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

        self.final_df['m_pattern'] = 'none'
        self.final_df['doc_descr'] = ''
        self.final_df['doc_descr'] = self.final_df.Document.str.extract('^(.*?);.*?$')
        self.final_df['m_pattern'] = self.final_df.Document.apply(lambda x: \
                self.match_regex_pattern(x,['ptn','charg','recurr','coverage','signal','ptn','number','price plan']))
        print("After pattern match [%s]" % (self.final_df.columns))
        self.final_df.to_csv(self.ddir + 'final_df1.csv')
        g_df = self.final_df[ \
                #(tcwa_eng_df.transferred == True) & \
                (self.final_df.m_pattern != 'other') \
                    ] \
                .groupby(['USER_ELID']) \
                ['EVENT_DT'].count() \
                .nlargest(50) \
                .reset_index(name='count') \
                .sort_values(['count'],ascending=False)
        print(g_df.head())
        g_df.to_csv( self.ddir + 'g_agent_id_' + self.o_fl, index=False)
        print("Unique Agents count [%d]" % (self.final_df.USER_ELID.nunique()))
        agent_id_df = self.final_df.USER_ELID.unique()
        out = csv.writer(open(self.ddir + 'agent_id_' + self.o_fl,"w"),delimiter=',')
        out.writerow(agent_id_df)

        #Group by additional variables.
        g_df = self.final_df[ \
                #(tcwa_eng_df.transferred == True) & \
                (self.final_df.m_pattern != 'other') \
                    ] \
                .groupby(['m_pattern','Search_Keyword','doc_descr']) \
                ['EVENT_DT'].count() \
                .nlargest(1000) \
                .reset_index(name='count') \
                .sort_values(['count'],ascending=False)
        print(g_df.head(25))
        g_df.to_csv( self.ddir + 'g_pattern_' + self.o_fl, index=False)
        print("Unique Agents count [%d]" % (self.final_df.USER_ELID.nunique()))

    
if __name__ == "__main__":
    cst = CST(ddir='../data/')

    files = ['cst_doc_usage_0228.csv','cst_search_query_0228.csv']

    #cst.load_files(files=files)
    #cst.get_ground_truth()
    cst.analyze_ground_truth()


