import time
import datetime as dt

'''
  my time keeper class.this class needs to be created as static instance at thread level. 
  For each interval get_time_passed needs to be invoked to get absolute milli-seconds since
  last get_time_passed call.
'''
class MyTimeKeeper:
    def __init__(self,token=None):
        self.my_milli = 0
        self.my_ts_token = {}
        if token:
            self.my_ts_token[token] = self.get_time_passed()
        else:
            self.get_time_passed()
    
    def get_time_passed(self, token = None):
        time_passed = 0
        new_time_milli = 0
        
        if self.my_milli == 0:
            self.my_milli = int(round(time.time() * 1000))
            time_passed = self.my_milli
            new_time_milli = self.my_milli
        else:
            new_time_milli = int(round(time.time() * 1000))
            time_passed = new_time_milli - self.my_milli
            self.my_milli = new_time_milli
            
        if token:
            if(self.my_ts_token.get(token,False)):
                time_passed = new_time_milli - self.my_ts_token[token]
                self.my_ts_token[token] = new_time_milli
            else:
                self.my_ts_token[token] = new_time_milli
            
        return time_passed
