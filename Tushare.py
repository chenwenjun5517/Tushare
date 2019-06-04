
#coding:utf-8

import tushare as ts
import  sys
import urllib
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

reload(sys)
sys.setdefaultencoding('utf8')

token = "91a4283aa5c9b78d7f369eb40705b8f2231a5a94c3cdaac7e3cdb5c2"

class Tushare(object):
    def __init__(self, code, startTime, endTime):
        self.token = token
        self.code = code
        self.startTime = startTime
        self.endTime = endTime

    def GetProfitDict(self):
        data = urllib.urlopen('https://tushare.pro/document/2?doc_id=33').read()
        data = data.decode("UTF-8")
        html = BeautifulSoup(data, 'html.parser')
        _dict = {}
        for tr in html.find_all("tr"):
            info_td_text = tr.get_text(strip=True).encode('utf-8')
            if info_td_text[0].isalpha():
                try:
                    _dict.update({info_td_text.split("str")[0]: info_td_text.split("str")[1][1:]})
                    continue
                except:
                    _dict.update({info_td_text.split("float")[0]: info_td_text.split("float")[1][1:]})
        del _dict['period']
        del _dict['start_date']
        return _dict

    def ConnectSQL(self):
        engine = create_engine('mssql+pymssql://chenwenjun:canwj@127.0.0.1/tushare?charset=utf8')
        return engine

    def GetProfitList(self):
        '''
        利润表
        '''
        profitDict = self.GetProfitDict()
        _fields = ", ".join(profitDict.keys()) #获得参数名称
        #print _fields
        pro = ts.pro_api(self.token)
       # print "ts_code=%s, start_date=%s,end_date=%s,fields=%s"%(self.code,self.startTime,self.endTime, "'" + _fields + "'")
        result = pro.income(ts_code = self.code,start_date = self.startTime, end_date = self.endTime,
                            fields = _fields)
        result = result.rename(columns=profitDict)
        result.to_excel("sh.xlsx")
        engine = self.ConnectSQL()
        result.to_sql("profitData", engine, if_exists= "append")
        print result



if __name__ =="__main__":
    # code = sys.argv[1]
    # startTime = sys.argv[2]
    # endTime = sys.argv[3]
    code = "600000.SH"
    startTime = "20180101"
    endTime = "20180730"
    Tushare = Tushare(code, startTime, endTime)
    Tushare.GetProfitList()
