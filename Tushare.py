
#coding:utf-8

import tushare as ts
import  sys
import urllib
import time
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

reload(sys)
sys.setdefaultencoding('utf8')

token = "91a4283aa5c9b78d7f369eb40705b8f2231a5a94c3cdaac7e3cdb5c2"
profitUrl = 'https://tushare.pro/document/2?doc_id=33'
assetsUrl = "https://tushare.pro/document/2?doc_id=36"

class Tushare(object):
    def __init__(self, code, startTime, endTime, listName):
        self.token = token
        self.code = code
        self.startTime = startTime
        self.endTime = endTime
        self.listName = listName
        self.profitUrl = profitUrl
        self.assetsUrl = assetsUrl

    def GetDict(self):

        dict_all = {"利润表":"income",
                    "资产负债表":"balancesheet",
                    "现金流量表":"cashflow"}
        return dict_all


    def GetProfitDict(self, _listName):
        if _listName == "income":
            data = urllib.urlopen(self.profitUrl).read()
        elif _listName == "balancesheet":
            data = urllib.urlopen(self.assetsUrl).read()
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

        delList = ["period","start_date","undi"]
        for _del in delList:
            if _del in _dict.keys():
                del _dict[_del]
        return _dict

    def ConnectSQL(self):
        engine = create_engine('mssql+pymssql://chenwenjun:canwj@127.0.0.1/tushare?charset=utf8')
        return engine

    def GetProfitList(self):
        '''
        利润表
        '''
        pro = ts.pro_api(self.token)

        for _code in self.code.split(","):
            for _listName in self.listName.split(","):
                profitDict = self.GetProfitDict(self.GetDict().get(_listName))
                _fields = ", ".join(profitDict.keys())  # 获得参数名称
                csvName = "%s-%s-%s.csv" % (time.strftime("%Y-%m-%d-%H-%M"),_code, _listName.decode("utf-8"))
                if self.GetDict().get(_listName) == "income":
                    result_profitList = pro.income(ts_code = _code,start_date = self.startTime, end_date = self.endTime,
                                                   fields = _fields)
                elif self.GetDict().get(_listName) == "balancesheet":
                    result_profitList = pro.balancesheet(ts_code=_code, start_date=self.startTime, end_date=self.endTime,
                                                   fields=_fields)
                result_profitList = result_profitList.rename(columns=profitDict)
                result_profitList.to_csv(csvName, mode = "a")


            #engine = self.ConnectSQL()
            #result.to_sql("profitData", engine, if_exists= "append")



if __name__ =="__main__":
    # code = sys.argv[1]
    # startTime = sys.argv[2]
    # endTime = sys.argv[3]
    # listName = sys.argv[4]
    code = "600000.SH,300030.SZ"
    startTime = "20180101"
    endTime = "20180730"
    listName = "利润表,资产负债表"
    Tushare = Tushare(code, startTime, endTime,listName)
    Tushare.GetProfitList()
