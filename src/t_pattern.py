import os
import pandas as pd
import datetime as dt
import re
import csv

_pattern = {
    'ptn': r'\bptn\b',
    'change': r'\bchange\b',
    'number': r'\bnumber\b',
    'area_code': r'\barea\s+code\b',
    'new': r'\bnew\b',
    'bill': r'\bbill\b',
    'charge': r'\bcharge',
    'monthly': r'\bmonthly\b',
    'reccur': r'\breccur',
    'price': r'\bprice\b',
    'plan': r'\bplan\b',
    'price_plan': r'\bprice\s+plan\b',
    'feature': r'\bfeature\b',
    'service': r'\bservice\b',
    'add_on': r'\b(add_on|add-on|addon)\b',
    'amazon': r'\bamazon\b',
    'prime': r'\bprime\b',
    'tidal': r'\btidal\b',
    'family': r'\bfamily\b',
    'locator': r'\blocator\b',
    'premium': r'\bpremium\b',
    'service': r'\bservice\b',
    'safe': r'\bsafe\b',
    'found': r'\bfound\b',
    'ip': r'\bip\b',
    'address': r'\baddress\b',
    'static': r'\bstatic\b',
    'futotv': r'\bfuto',
    'hulu': r'\bhulu\b',
    'voicemail': r'\b(voicemail|voice mail)\b',
    'visualvoice': r'\b(visualvoice|visual voice)\b',
    'promotion': r'\b(promotion|promo)\b',
    'buy': r'\bbuy',
    'back': r'back\b',
    'free': r'\bfree\b',
    'preorder': r'\b(preorder|pre-order)\b',
    'device': r'\bdevice\b',
    'bogo': r'\bbogo\b',
    'unlimited': r'\bunlimited\b',
    'lookout': r'\blookout\b',
    'offer': r'\boffer\b',
    'coverage': r'\bcoverage\b',
    'signal': r'\bsignal\b',
    'call': r'\bcall\b',
    'network': r'\bnetwork\b',
    'negate': r'\b(no|problem|issue|unavailable|troubleshoot|not working|bad|poor)\b',
    'disconnect': r'\bdisconnect',
    'frequent': r'\bfrequent',
    'reception': r'\breception\b',
    'drop': r'\bdrop'
}

_patterns = {
    'change_ptn': {
        'change_ptn': ['ptn','change'],
        'change_number': ['number','change'],
        'area_code': ['area_code'],
        'new_number': ['number','new']
    },
    'charge_inquiry': {
        'monthly_charge': ['charge','monthly'],
        'monthly_reccuring': ['monthly','reccur'],
        'monthly_reccuring_charges': ['monthly','reccur','charge'],
        'price_plan': ['price','plan'],
        'price_plan_charge': ['price_plan','charge'],
        'unlimited': ['unlimited'],
        'bogo_offer': ['bogo','offer'],
        'promotion_offer': ['promotion','offer'],
        'device_promotion': ['device','promotion'],
        'free_promotion': ['free','promotion'],
        'buy_back_promotion': ['buy','back','promotion'],
        'buy_promotion': ['buy','promotion'],
        'preorder_promotion': ['preorder','promotion'],
        'plan': ['plan'],
        'charge': ['charge']
    },
    'additional_service': {
        'amazon_prime': ['amazon','prime'],
        'tidal': ['tidal'],
        'family_locator': ['family','locator'],
        'lookout': ['lookout'],
        'premium_service': ['premium','service'],
        'safe_found': ['safe','found'],
        'static_ip': ['static','ip'],
        'ip_address': ['address','ip'],
        'hulu': ['hulu'],
        'futo_tv': ['futotv'],
        'voice_mail': ['voicemail'],
        'visual_voice': ['visualvoice']
    },
    'coverage_inquiry': {
        'coverage': ['coverage'],
        'signal': ['signal'],
        'network': ['negate','network'],
        'no_service': ['negate','service'],
        'call_disconnect': ['call','disconnect'],
        'frequent_disconnect': ['frequent','disconnect'],
        'drop': ['drop'],
        'no_reception': ['negate','reception']
    }
}

class Result:
    def __init__(self,id,match_id,src_str=None):
        self._res = {}
        self._res['id'] = id
        self._res['mid'] = match_id
        if src_str:
            self._res['src_str'] = src_str
        else:
            self._res['src_str'] = ''
        self.result_str = "id[%s] match_ids[%s] src_str[%s]"

    def get_result_str(self):
        return self.result_str % (self._res['id'],self._res['mid'],self._res['src_str'])

    def get_result(self):
        return self._res['id'],self._res['mid']

class ResultSet:
    def __init__(self,id,ddir='../data/'):
        self._id = id
        self.resultset = {}
        self.cols = ['key','m_pattern','m_sub_pattern']
        self.ddir = ddir
        self.o_fl = 'm_pattern_' + id + '.csv'

    def update_result(self,id,match_id,src_str=None):
        if id in self.resultset:
            '''
            result = self.resultset[id]
            result.update_match(id,match_id)
            '''
            return False
        else:
            result = Result(id,match_id,src_str)
            self.resultset[id] = result

            return True
    
    def get_resultset_as_df(self):
        id_arr = []
        m_pattern_arr = []
        m_sub_pattern_arr = []
        default_sub_pattern = 'NA'

        for key in self.resultset:
            id, m_pattern = self.resultset[key].get_result()
            id_arr.append(id)
            pat = m_pattern.split('-') #extract from "pattern-subpattern"
            m_pattern_arr.append(pat[0])
            try:
                m_sub_pattern_arr.append(pat[1])
            except IndexError:
                m_sub_pattern_arr.append(default_sub_pattern)

        df = pd.DataFrame(columns=self.cols)
        df[self.cols[0]] = id_arr
        df[self.cols[1]] = m_pattern_arr
        df[self.cols[2]] = m_sub_pattern_arr

        #print("Pattern Match [%d] results" % (df[self.cols[0]].count()))
        #print(df.head())
        df.to_csv( self.ddir + self.o_fl, index=False)

        return df

    def print_resultset(self,no_rec=5):
        print("Printing ResultSet for [%s]" % (self._id))
        print("-----------------------------------------------------------------")
        i = 0
        for key in self.resultset:
            if no_rec < 1:
                print(self.resultset[key].get_result_str())
            else:
                if i >= no_rec:
                    break
                else:
                    print(self.resultset[key].get_result_str())
            i += 1

class Pattern:
    def __init__(self,id,default_match="no_match"):
        print("Pattern initiated...")
        self.id = id
        self.pat_dict = _pattern
        self.default_match = default_match

    def update_for_pattern_match(self,s_key,sent):
        sent_dict = {}
        for uc_key in _patterns:
            p_dict = _patterns[uc_key]
            for p_key in p_dict:
                m_cnt = 0
                m_key = "%s-%s" % (uc_key,p_key)
                #print("++%s -> %s -> [%s]" % (uc_key,p_key,",".join(p_dict[p_key]))) 
                for p_id in p_dict[p_key]:
                    if p_id in _pattern:
                        m = re.search(_pattern[p_id],sent,re.I)
                        if m:
                            #print("Checking for [%s] [%s] [%s]" % (p_id,_pattern[p_id],m.groups()))
                            m_cnt += 1
                    else:
                        print("@@[%s] pattern not defined in _pattern" % (p_id))
                if m_cnt >= len(p_dict[p_key]):
                    if s_key not in sent_dict:
                        sent_dict[s_key] = m_key
                        #print("****Matched k[%s] [%d/%d] [%s]" % (m_key,m_cnt,len(p_dict[p_key]),sent)) 
                #print("****Matched k[%s] [%d/%d] [%s]" % (m_key,m_cnt,len(p_dict[p_key]),sent)) 
        if s_key not in sent_dict:
            #print("####Match failed for [%s]" % (sent))
            self.rs.update_result( id=s_key,match_id=self.default_match,src_str=sent)
        else:
            self.rs.update_result( id=s_key,match_id=sent_dict[s_key],src_str=sent)

    def scan_dict_for_pattern_match(self,sent_dict):
        sent_res = {}
        self.rs = ResultSet(id="rel-1")
        for s_key in sent_dict:
            sent = sent_dict[s_key]
            #print("Processing [%s] [%s]" % (s_key,sent))
            self.update_for_pattern_match(s_key,sent)
        self.rs.print_resultset()
        
        return self.rs.get_resultset_as_df()

    def scan_df_for_pattern_match(self,sent_df):
        self.rs = ResultSet(id=self.id)
        for i,rec in sent_df.iterrows():
            #print(rec.key,rec.doc_descr)
            self.update_for_pattern_match(rec.key,rec.doc_descr)
        self.rs.print_resultset()
        
        return self.rs.get_resultset_as_df()


if __name__ == '__main__':
    p_dict = {
        'ptn1': 'change my number',
        'ptn2': 'i want number',
        'ptn3': 'i want new number',
        'chrg1': 'what are my monthly reccuring hulu charges',
        'chrg2': 'what are details for hulu charges',
        'chrg3': 'what are different price plan for hotspot service charges',
        'vv1': 'cancel my visual voice',
        'vv2': 'i want to activate visualvoice service',
        'cai1': 'bad network',
        'cai2': 'my call getting droped',
        'cai3': 'i do not have signal at my office'
    }
    pat = Pattern(id='test1',default_match="other")
    #pat.test_pattern_dict(sent_dict=p_dict)
    #df = pat.scan_dict_for_pattern_match(sent_dict=p_dict)
    i_df = pd.DataFrame(list(p_dict.items()),columns=['key','doc_descr'])
    df = pat.scan_df_for_pattern_match(sent_df=i_df)
    print(df.head(10))



