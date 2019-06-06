
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
stockBasicUrl = 'https://tushare.pro/document/2?doc_id=25'
tradeCal = "https://tushare.pro/document/2?doc_id=26"

class TushareBaseData(object):
    def __init__(self, listName, startDate, endDate):
        self.token = token
        self.listName = listName
        self.startDate = startDate
        self.endDate = endDate
        self.stockBasicUrl = stockBasicUrl
        self.tradeCal = tradeCal
        self.pro = ts.pro_api(self.token)

    def GetDict(self):

        dict_all = {"股票列表":"stock_basic",
                    "交易日历":"trade_cal",
                    "现金流量表":"cashflow"}
        return dict_all

    def GetProfitDict(self, _listName):

        if _listName == "stock_basic":
            data = urllib.urlopen(self.stockBasicUrl).read()
        elif _listName == "trade_cal":
            data = urllib.urlopen(self.tradeCal).read()
        data = data.decode("UTF-8")
        html = BeautifulSoup(data, 'html.parser')
        _dict = {}
        for tr in html.find_all("tr"):
            info_td_text = tr.get_text(strip=True).encode('utf-8')
            if info_td_text[0].isalpha():
                try:
                    _dict.update({info_td_text.rsplit("str",1)[0]: info_td_text.rsplit("str",1)[1][1:]})
                    continue
                except:
                    _dict.update({info_td_text.rsplit("float",1)[0]: info_td_text.rsplit("float",1)[1][1:]})

        return _dict

    def ConnectSQL(self):
        engine = create_engine('mssql+pymssql://chenwenjun:canwj@127.0.0.1/tushare?charset=utf8')
        return engine

    def stockBasic(self):
        '''
        基础数据
        '''
        # engine = self.ConnectSQL()
        pro = self.pro
        for _listName in self.listName.split(","):
            profitDict = self.GetProfitDict(self.GetDict().get(_listName))
            _fields = ", ".join(profitDict.keys())  # 获得参数名称
            csvName = "%s-%s.csv" % (time.strftime("%Y-%m-%d-%H-%M"), _listName.decode("utf-8"))
            if self.GetDict().get(_listName) == "stock_basic": #股票列表
                result = pro.stock_basic(exchange='', list_status='L',fields = _fields)

            elif self.GetDict().get(_listName) == "trade_cal": # 交易日历
                result = pro.trade_cal(exchange='', start_date=self.startDate, end_date=self.endDate)

            result.to_csv(csvName, mode = "ab+")
            #result.to_sql("profitData", engine, if_exists= "append")

    def nameChange(self):
        '''
        曾用名
        '''
        pro = self.pro
        result = pro.stock_basic(exchange='', list_status='L',fields = "ts_code")
        csvName = "%s-%s.csv" % (time.strftime("%Y-%m-%d-%H-%M"), "nameChange")
        for tsCode in result["ts_code"]:
            nameChange_result = pro.namechange(ts_code=tsCode, fields='ts_code,name,start_date,end_date,change_reason')
            print nameChange_result
            nameChange_result.to_csv(csvName, mode = "ab+")




if __name__ =="__main__":
    # code = sys.argv[1]
    # startTime = sys.argv[2]
    # endTime = sys.argv[3]
    # listName = sys.argv[4]
    listName = "股票列表,交易日历"
    startDate = "20180101"
    endDate = "20181231"
    TushareBaseData = TushareBaseData(listName, startDate, endDate)
    TushareBaseData.stockBasic()
    TushareBaseData.nameChange()
