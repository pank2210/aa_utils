import json
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, CategoriesOptions, EntitiesOptions, ConceptsOptions,KeywordsOptions, SemanticRolesOptions

api_cred = {
  "apikey": "1IJHzX6xD3nSceULZuGwC_4wFZ0XqFL5euEiyknVE_dJ",
  "iam_apikey_description": "Auto-generated for key dc4cf0f7-8bcd-4acc-8540-5dad9257d402",
  "iam_apikey_name": "Auto-generated service credentials",
  "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Manager",
  "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/0440feb8201146deaab3d0f37b89fba8::serviceid:ServiceId-84ee690f-0727-4601-9044-d13391e5109a",
  "url": "https://gateway.watsonplatform.net/natural-language-understanding/api"
}

class NLU:
  def log(self,fname,level=2,str=''):
    if level <= self.debug_level:
       print( "[%s::%s]|[%s]" %(self.cname,fname,str))
  
  def __init__(self,cred=None):
    self.debug_level = 2
    self.cname = 'NLU'
    fname = '__init__'
    
    self.log(fname,2,' call entry')
    self.natural_language_understanding = NaturalLanguageUnderstandingV1( \
            version='2018-11-16', \
            iam_apikey = api_cred['apikey'], \
            url = api_cred['url'] \
         ) \
    
  
  def invoke( self, i_text):
    fname = 'invoke'
     
    self.log(fname,2,' call entry')
    response = self.natural_language_understanding.analyze( \
         text = i_text,
         features=Features(
                              #categories=CategoriesOptions(limit=3), \
                              #concepts=ConceptsOptions(limit=5), \
                              #keywords=KeywordsOptions(limit=10), \
                              #semantic_roles=SemanticRolesOptions(limit=3), \
                              entities=EntitiesOptions(limit=249) \
                            ) \
       ).get_result()
     
    resp_obj = json.dumps(response, indent=2)
    self.log(fname,1,resp_obj)
    o_fd = open("./out/nlu.out",'a')
    o_fd.write("##request[%s]##response[%s]\n" % (i_text,resp_obj))
    o_fd.close()
    
    return resp_obj

if __name__ == '__main__':
  print('NLU Tester')
  text = "If you want to acheive success, billionaire technology entrepreneur and investor on ABC's \"Shark Tank\" Mark Cuban has some advice: always challenge yourself to absorb new information .\n\"Life-long learning is probably the greatest skill,\" he says on Arianna Huffington's The Thrive Global Podcast .\nCuban is an avid reader , and espouses the benefits of scouring books for ideas. Early in his career, reading up on the technology industry helped him get an edge on competitors.\n\"A guy with little computer background could compete with far more experienced guys, just because I put in the time to learn all I could,\" he writes on his blog. \"Everything I read was public. Anyone could buy the same books and magazines. The same information was available to anyone who wanted it. Turns out most people didn't want it.\"\nEven with a net worth of over $3 billion today, he's still reading.\nRecently, Cuban read a copy of \"Principles: Life and Work\" by Ray Dalio, the founder of Bridgewater Associates. Dalio's firm is the largest hedge fund in the world, and manages $160 billion.\nshow chapters Billionaire hedge fund founder Ray Dalio uses 'radical transparency' to deliver feedback—here's how it works 9:12 AM ET Wed, 13 Sept 2017 | 01:18 In \"Principles,\" Dalio explains step by step how he sought to create a culture of brutal honesty at Bridgewater, so the best ideas would rise to the top. The book explains the principles Dalio and all his employees follow, including \"evaluate accurately, not kindly,\" and \"don't hide your observations about people.\"\nCuban says the book would have been helpful to learn from as he was starting out.\nTweet\n\"It's the book I wish I had as a young entrepreneur, stressing over not knowing what I didn't know,\" Cuban tweeted on Dec. 31.\nMore importantly, Cuban says \"Principles\" gives instruction on how to develop the skill he values as critical to success: becoming a perpetual learner.\n\"'Principles' offers a bible to the greatest skill an entrepreneur can have, the ability to learn how to learn in any situation,\" Cuban writes. \"Read it.\"\nDon't miss: Mark Cuban: The 3 best tips to save more money in 2018\nshow chapters \"It scares the s*** out of me,\" billionaire Mark Cuban says of AI 1:37 PM ET Tue, 25 July 2017 | 00:59 Like this story? Like CNBC Make It on Facebook !"
   
  nlu = NLU()
  nlu.invoke(text)
