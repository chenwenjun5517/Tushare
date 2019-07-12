
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
incomeUrl = 'https://tushare.pro/document/2?doc_id=33'
balancesheetUrl = "https://tushare.pro/document/2?doc_id=36"
cashflowUrl = "https://tushare.pro/document/2?doc_id=44"
dividendUrl = "https://tushare.pro/document/2?doc_id=103"
fina_indicatorUrl = "https://tushare.pro/document/2?doc_id=79"
fina_auditUrl = "https://tushare.pro/document/2?doc_id=80"
fina_mainbzUrl = "https://tushare.pro/document/2?doc_id=81"
disclosure_dateUrl = "https://tushare.pro/document/2?doc_id=162"

class Tushare(object):
    def __init__(self, code, startTime, endTime, listName):
        self.token = token
        self.code = code
        self.startTime = startTime
        self.endTime = endTime
        self.listName = listName
        self.incomeUrl = incomeUrl
        self.balancesheetUrl = balancesheetUrl
        self.cashflowUrl = cashflowUrl
        self.dividendUrl = dividendUrl
        self.fina_indicatorUrl = fina_indicatorUrl
        self.fina_auditUrl = fina_auditUrl
        self.fina_mainbzUrl = fina_mainbzUrl
        self.disclosure_dateUrl = disclosure_dateUrl

    def GetDict(self):

        dict_all = {"利润表":"income",
                    "资产负债表":"balancesheet",
                    "现金流量表":"cashflow",
                    "分红送股":"dividend",
                    "财务指标数据":"fina_indicator",
                    "财务审计意见":"fina_audit",
                    "主营业务构成":"fina_mainbz",
                    "财报披露计划":"disclosure_date"}
        return dict_all


    def GetProfitDict(self, _listName):
        if _listName == "income":
            data = urllib.urlopen(self.incomeUrl).read()
        elif _listName == "balancesheet":
            data = urllib.urlopen(self.balancesheetUrl).read()
        elif _listName == "cashflow":
            data = urllib.urlopen(self.cashflowUrl).read()
        elif _listName == "dividend":
            data = urllib.urlopen(self.dividendUrl).read()
        elif _listName == "fina_indicator":
            data = urllib.urlopen(self.fina_indicatorUrl).read()
        elif _listName == "fina_audit":
            data = urllib.urlopen(self.fina_auditUrl).read()
        elif _listName == "fina_mainbz":
            data = urllib.urlopen(self.fina_mainbzUrl).read()
        elif _listName == "disclosure_date":
            data = urllib.urlopen(self.disclosure_dateUrl).read()
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

    def ConnectMySQL(self):
        engine = create_engine('mysql://tushare:erahsuT=@rm-wz9512zuh2109gv5qmo.mysql.rds.aliyuncs.com:3306/tushare?charset=utf8')
        return engine

    def GetProfitList(self):
        '''
        利润表
        '''
        pro = ts.pro_api(self.token)
        engine = self.ConnectMySQL()
        for _code in self.code.split(","):
            for _listName in self.listName.split(","):
                profitDict = self.GetProfitDict(self.GetDict().get(_listName)) # 对应中文名
                _fields = ", ".join(profitDict.keys())  # 获得参数名称
                csvName = "%s-%s-%s.csv" % (time.strftime("%Y-%m-%d-%H-%M"),_code, _listName.decode("utf-8"))
                if self.GetDict().get(_listName) == "income": # 利润表
                    result_profitList = pro.income(ts_code = _code,start_date = self.startTime, end_date = self.endTime,
                                                   fields = _fields)
                elif self.GetDict().get(_listName) == "balancesheet":     # 资产负载表
                    result_profitList = pro.balancesheet(ts_code=_code, start_date=self.startTime, end_date=self.endTime,
                                                   fields=_fields)

                elif self.GetDict().get(_listName) == "cashflow":         # 现金流量表
                    result_profitList = pro.cashflow(ts_code= _code, start_date=self.startTime, end_date=self.endTime)

                elif self.GetDict().get(_listName) == "dividend":         # 分红送股数据
                    result_profitList = pro.dividend(ts_code=_code, fields=_fields)

                elif self.GetDict().get(_listName) == "fina_indicator":   # 财务指标数据
                    result_profitList = pro.fina_indicator(ts_code=_code)

                elif self.GetDict().get(_listName) == "fina_audit":       # 财务审计意见
                    result_profitList = pro.fina_audit(ts_code=_code, start_date=self.startTime, end_date=self.endTime)

                elif self.GetDict().get(_listName) == "fina_mainbz":      # 主营业务构成
                    result_profitList = pro.fina_mainbz(ts_code=_code, type='P')

                elif self.GetDict().get(_listName) == "disclosure_date":  # 财报披露日期表
                    result_profitList = pro.disclosure_date(end_date=self.endTime)
                #print self.GetDict().get(_listName)
                #result_profitList = result_profitList.rename(columns=profitDict)
                #result_profitList.to_csv(csvName, mode = "ab+")
                result_profitList.to_sql(self.GetDict().get(_listName), engine, if_exists= "append")



if __name__ =="__main__":
    # code = sys.argv[1]
    # startTime = sys.argv[2]
    # endTime = sys.argv[3]
    # listName = sys.argv[4]
    code = "600000.SH,300030.SZ"
    startTime = "20180101"
    endTime = "20180730"
    listName = "利润表,资产负债表,现金流量表,分红送股,财务指标数据,财务审计意见,主营业务构成,财报披露计划"
    Tushare = Tushare(code, startTime, endTime,listName)
    Tushare.GetProfitList()
