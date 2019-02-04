# -*- coding:utf-8 -*-
"""
投资参考类
Created on 2019/01/03
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""

from __future__ import division

import math
import time
import pandas as pd
from pandas.compat import StringIO
import lxml.html
from lxml import etree
import re
import json
from utility import Utility
from base import Base, cf

class Reference(Base):
    def distriPlan(self, year=2015, top=25, retry=3, pause=0.001):
        """
        获取分配预案数据
        Parameters
        --------
        year:年份
        top:取最新n条数据，默认取最近公布的25条
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        returns
        -------
        DataFrame or List: [{'code', 'name', ...}, ...]
        code:股票代码
        name:股票名称
        year:分配年份
        report_date:公布日期
        divi:分红金额（每10股）
        shares:转增和送股数（每10股）
        """
        self._data = pd.DataFrame()
        
        if top == 'all':
            self._writeHead()
            
            self._data, pages = self.__handleDistriPlan(year, 0, retry, pause)
            for i in range(1, int(pages)):
                self._data = self._data.append(self.__handleDistriPlan(year, i, retry, pause), ignore_index=True)
                
            return self._result()
        elif top <= 25:
            self._data, pages = self.__handleDistriPlan(year, 0, retry, pause)
            self._data = self._data.head(top)
            
            return self._result()
        else:
            if isinstance(top, int):
                self._writeHead()
                
                allPages = int(math.ceil(top/25))
                self._data, pages = self.__handleDistriPlan(year, 0, retry, pause)
                pages = min(allPages, int(pages))
                for i in range(1, pages):
                    self._data = self._data.append(self.__handleDistriPlan(year, i, retry, pause), ignore_index=True)
                
                self._data = self._data.head(top)
                
                return self._result()
            else:
                print(cf.TOP_PARAS_MSG)
    
    
    def __handleDistriPlan(self, year, pageNo, retry, pause):
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                if pageNo > 0:
                    self._writeConsole()
                    
                # http://quotes.money.163.com/data/caibao/fpyg.html?reportdate=2018&sort=declaredate&order=desc&page=0
                html = lxml.html.parse(cf.DP_163_URL % (year, pageNo))  
                res = html.xpath('//table[@class=\"fn_cm_table\"]/tr')
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = '<table>%s</table>' % sarr
                df = pd.read_html(sarr)[0]
                df = df.drop(0, axis=1)
                df.columns = cf.DP_163_COLS
                df['divi'] = df['plan'].map(self.__bonus)
                df['shares'] = df['plan'].map(self.__gift)
                df = df.drop('plan', axis=1)
                df['code'] = df['code'].astype(object)
                df['code'] = df['code'].map(lambda x : str(x).zfill(6))
                pages = []
                if pageNo == 0:
                    page = html.xpath('//div[@class=\"mod_pages\"]/a')
                    if len(page)>1:
                        asr = page[len(page)-2]
                        pages = asr.xpath('text()')
            except Exception as e:
                print(e)
            else:
                if pageNo == 0:
                    return df, pages[0] if len(pages)>0 else 0
                else:
                    return df
                
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def __bonus(self, x):
        if self._PY3:
            reg = re.compile(r'分红(.*?)元', re.UNICODE)
            res = reg.findall(x)
            return 0 if len(res)<1 else float(res[0]) 
        else:
            if isinstance(x, unicode):
                s1 = unicode('分红','utf-8')
                s2 = unicode('元','utf-8')
                reg = re.compile(r'%s(.*?)%s'%(s1, s2), re.UNICODE)
                res = reg.findall(x)
                return 0 if len(res)<1 else float(res[0])
            else:
                return 0
            
            
    def __gift(self, x):
        if self._PY3:
            reg1 = re.compile(r'转增(.*?)股', re.UNICODE)
            reg2 = re.compile(r'送股(.*?)股', re.UNICODE)
            res1 = reg1.findall(x)
            res2 = reg2.findall(x)
            res1 = 0 if len(res1)<1 else float(res1[0])
            res2 = 0 if len(res2)<1 else float(res2[0])
            
            return res1 + res2
        else:
            if isinstance(x, unicode):
                s1 = unicode('转增','utf-8')
                s2 = unicode('送股','utf-8')
                s3 = unicode('股','utf-8')
                reg1 = re.compile(r'%s(.*?)%s'%(s1, s3), re.UNICODE)
                reg2 = re.compile(r'%s(.*?)%s'%(s2, s3), re.UNICODE)
                res1 = reg1.findall(x)
                res2 = reg2.findall(x)
                res1 = 0 if len(res1)<1 else float(res1[0])
                res2 = 0 if len(res2)<1 else float(res2[0])
                
                return res1 + res2
            else:
                return 0
            
            
    def forecast(self, year, quarter, retry=3, pause=0.001):
        """
        获取业绩预告数据
        Parameters
        --------
        year:int 年度 e.g:2014
        quarter:int 季度 :1、2、3、4，只能输入这4个季度
           说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题   
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code,代码
            name,名称
            type,业绩变动类型【预增、预亏等】
            report_date,发布日期
            pre_eps,上年同期每股收益
            range,业绩变动范围
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            self._data =  self.__handleForecast(year, quarter, 1, pd.DataFrame(), retry, pause)
            self._data = pd.DataFrame(self._data, columns=cf.FORECAST_COLS)
            self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
            
            return self._result()
        
        
    def __handleForecast(self, year, quarter, pageNo, dataArr, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/performance/index.phtml?s_i=&s_a=&s_c=&s_type=&reportdate=2018&quarter=3&p=1&num=60
                request = self._session.get( cf.FORECAST_URL%( year, quarter, pageNo, cf.PAGE_NUM[1]), timeout=10 )
                request.encoding = 'gbk'
                text = request.text.replace('--', '')
                html = lxml.html.parse(StringIO(text))
                res = html.xpath("//table[@class=\"list_table\"]/tr")
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = '<table>%s</table>'%sarr
                df = pd.read_html(sarr)[0]
                df = df.drop([4, 5, 8], axis=1)
                df.columns = cf.FORECAST_COLS
                dataArr = dataArr.append(df, ignore_index=True)
                nextPage = html.xpath('//div[@class=\"pages\"]/a[last()]/@onclick')
                if len(nextPage)>0:
                    pageNo = re.findall(r'\d+',nextPage[0])[0]
                    return self.__handleForecast(year, quarter, pageNo, dataArr, retry, pause)
                else:
                    return dataArr
            except Exception as e:
                    print(e)
                    
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def restrictedLift(self, year=None, month=None, retry=3, pause=0.001):
        """
        获取限售股解禁数据
        Parameters
        --------
        year:年份,默认为当前年
        month:解禁月份，默认为当前月
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
        code:股票代码
        name:名称
        date:解禁日期
        count:解禁数量（万股）
        ratio:占总盘比率
        """
        self._data = pd.DataFrame()
        
        year = Utility.getYear() if year is None else year
        month = Utility.getMonth() if month is None else month
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                # http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=FD&sty=BST&st=3&sr=true&fd=2019&stat=1
                request = self._session.get( cf.RL_URL % (year, month), timeout = 10 )
                if self._PY3:
                    request.encoding = 'utf-8'
                lines = request.text
            except Exception as e:
                print(e)
            else:
                da = lines[3:len(lines)-3]
                list =  []
                for row in da.split('","'):
                    list.append([data for data in row.split(',')])
                self._data = pd.DataFrame(list)
                self._data = self._data[[1, 3, 4, 5, 6]]
                for col in [5, 6]:
                    self._data[col] = self._data[col].astype(float)
                self._data[5] = self._data[5]/10000
                self._data[6] = self._data[6]*100
                self._data[5] = self._data[5].map(cf.FORMAT)
                self._data[6] = self._data[6].map(cf.FORMAT)
                self._data.columns = cf.RL_COLS
                
                return self._result()
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)


    def fundHoldings(self, year, quarter, retry=3, pause=0.001):
        """
        获取基金持股数据
        Parameters
        --------
        year:年份e.g 2014
        quarter:季度（只能输入1，2，3，4这个四个数字）
        retry : int, 默认 3
                    如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
        code:股票代码
        name:名称
        date:报告日期
        nums:基金家数
        nlast:与上期相比（增加或减少了）
        count:基金持股数（万股）
        clast:与上期相比
        amount:基金持股市值
        ratio:占流通盘比率
        """
        self._data = pd.DataFrame()
        
        start, end = cf.QUARTS_DIC[str(quarter)]
        if quarter == 1:
            start = start % str(year-1)
            end = end % year
        else:
            start, end = start % year, end % year

        self._writeHead()

        self._data, pages = self.__handleFoundHoldings(start, end, 0, retry, pause)
        for idx in range(1, pages):
            self._data = self._data.append(self.__handleFoundHoldings(start, end, idx, retry, pause), ignore_index=True)

        return self._result()


    def __handleFoundHoldings(self, start, end, pageNo, retry, pause):
        for _ in range(retry):
            time.sleep(pause)
            if pageNo>0:
                    self._writeConsole()
            try:
                # http://quotes.money.163.com/hs/marketdata/service/jjcgph.php?host=/hs/marketdata/service/jjcgph.php&page=0&query=start:2018-06-30;end:2018-09-30&order=desc&count=60&type=query&req=73259
                request = self._session.get( cf.FUND_HOLDS_URL % (pageNo, start, end, Utility.random(5)), timeout=10 )
                if self._PY3:
                    request.encoding = 'utf-8'
                lines = request.text
                lines = lines.replace('--', '0')
                lines = json.loads(lines)
                data = lines['list']
                df = pd.DataFrame(data)
                df = df.drop(['CODE', 'ESYMBOL', 'EXCHANGE', 'NAME', 'RN', 'SHANGQIGUSHU', 'SHANGQISHIZHI', 'SHANGQISHULIANG'], axis=1)
                for col in ['GUSHU', 'GUSHUBIJIAO', 'SHIZHI', 'SCSTC27']:
                    df[col] = df[col].astype(float)
                df['SCSTC27'] = df['SCSTC27']*100
                df['GUSHU'] = df['GUSHU']/10000
                df['GUSHUBIJIAO'] = df['GUSHUBIJIAO']/10000
                df['SHIZHI'] = df['SHIZHI']/10000
                df['GUSHU'] = df['GUSHU'].map(cf.FORMAT)
                df['GUSHUBIJIAO'] = df['GUSHUBIJIAO'].map(cf.FORMAT)
                df['SHIZHI'] = df['SHIZHI'].map(cf.FORMAT)
                df['SCSTC27'] = df['SCSTC27'].map(cf.FORMAT)
                df.columns = cf.FUND_HOLDS_COLS
                df = df[['code', 'name', 'date', 'nums', 'nlast', 'count', 
                            'clast', 'amount', 'ratio']]
            except Exception as e:
                print(e)
            else:
                if pageNo == 0:
                    return df, int(lines['pagecount'])
                else:
                    return df

        raise IOError(cf.NETWORK_URL_ERROR_MSG)


    def ipo(self, retry=3, pause=0.001):
        """
        获取新股上市数据
        Parameters
        --------
        retry : int, 默认 3
                    如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
        code:股票代码
        xcode:申购代码
        name:名称
        ipo_date:上网发行日期
        issue_date:上市日期
        amount:发行数量(万股)
        markets:上网发行数量(万股)
        price:发行价格(元)
        pe:发行市盈率
        limit:个人申购上限(万股)
        funds：募集资金(亿元)
        ballot:网上中签率(%)
        """
        self._data = pd.DataFrame()

        self._writeHead()

        self._data = self.__handleIpo(self._data, 1, retry, pause)

        return self._result()


    def __handleIpo(self, data, pageNo, retry, pause):
        self._writeConsole()

        for _ in range(retry):
            time.sleep(pause)

            try:
                # http://vip.stock.finance.sina.com.cn/corp/view/vRPD_NewStockIssue.php?page=1&cngem=0&orderBy=NetDate&orderType=desc
                html = lxml.html.parse(cf.NEW_STOCKS_URL % pageNo)
                res = html.xpath('//table[@id=\"NewStockTable\"]/tr')
                if not res:
                    return data
                
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = sarr.replace('<font color="red">*</font>', '')
                sarr = '<table>%s</table>'%sarr
                df = pd.read_html(StringIO(sarr), skiprows=[0, 1])[0]
                df = df.drop([df.columns[idx] for idx in [12, 13, 14]], axis=1)
                df.columns = cf.NEW_STOCKS_COLS
                df['code'] = df['code'].map(lambda x : str(x).zfill(6))
                df['xcode'] = df['xcode'].map(lambda x : str(x).zfill(6))
                res = html.xpath('//table[@class=\"table2\"]/tr[1]/td[1]/a/text()')
                tag = '下一页' if self._PY3 else unicode('下一页', 'utf-8')
                hasNext = True if tag in res else False 
                data = data.append(df, ignore_index=True)
                pageNo += 1
                if hasNext:
                    data = self.__handleIpo(data, pageNo, retry, pause)
            except Exception as ex:
                print(ex)
            else:
                return data
            
            
    def shMargins(self, retry=3, pause=0.001):
        """
        沪市融资融券历史数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'date':, 'close':, ...}, ...]
            date: 日期
            close: 上证指数收盘点数
            zdf: 上证指数收盘涨跌幅(%)
            rzye: 融资余额(元)
            rzyezb: 融资余额占比(%)
            rzmre: 融资买入额(元)
            rzche: 融资偿还额(元)
            rzjmre: 融资净买入额(元)
            rqye: 融券余额(元)
            rqyl: 融券余量(股)
            rqmcl: 融券卖出量(股)
            rqchl: 融券偿还量(股)
            rqjmcl: 融券净卖出量(股)
            rzrqye: 融资融券余额(元)
            rzrqyecz: 融资融券余额差值(元)
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleMargins(self._data, 1, 'SH', Utility.random(8), cf.MAR_COLS, retry, pause)
        self._data.rename(columns={'tdate':'date'}, inplace=True)
        
        return self._result()
    
    
    def szMargins(self, retry=3, pause=0.001):
        """
        深市融资融券历史数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'date':, 'close':, ...}, ...]
            date: 日期
            close: 深证成指收盘点数
            zdf: 深证成指收盘涨跌幅(%)
            rzye: 融资余额(元)
            rzyezb: 融资余额占比(%)
            rzmre: 融资买入额(元)
            rzche: 融资偿还额(元)
            rzjmre: 融资净买入额(元)
            rqye: 融券余额(元)
            rqyl: 融券余量(股)
            rqmcl: 融券卖出量(股)
            rqchl: 融券偿还量(股)
            rqjmcl: 融券净卖出量(股)
            rzrqye: 融资融券余额(元)
            rzrqyecz: 融资融券余额差值(元)
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleMargins(self._data, 1, 'SZ', Utility.random(8), cf.MAR_COLS, retry, pause)
        self._data.rename(columns={'tdate':'date'}, inplace=True)
        
        return self._result()
    
    
    def __handleMargins(self, dataArr, page, market, randInt, column, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get( cf.MAR_URL % (page, market, randInt) )
                text = request.text.split('=')[1]
                text = text.replace('{pages:', '{"pages":').replace(',data:', ',"data":').replace('T00:00:00', '').replace('"-"', '0')
                dataDict = Utility.str2Dict(text)
                data = dataDict['data']
                df = pd.DataFrame(data, columns=column)
                
                df['close'] = df['close'].map(cf.FORMAT)
                df['rzyezb'] = df['rzyezb'].astype(float)
                
                dataArr = dataArr.append(df, ignore_index=True)
                if page < dataDict['pages']:
                    dataArr = self.__handleMargins(dataArr, page+1, market, randInt, column, retry, pause)
            except Exception as e:
                print(e)
            else:
                return dataArr
                
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def marginDetailsAllByDate(self, date, retry=3, pause=0.001):
        """
        按日期获取两市融资融券明细列表
        Parameters
        --------
        date  : string
                     选择日期 format：YYYY-MM-DD
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code: 股票代码
            name: 名称
            rzye: 当日融资余额(元)
            rzyezb: 当日融资余额占比(%)
            rzmre: 当日融资买入额(元)
            rzche: 当日融资偿还额(元)
            rzjmre: 当日融资净买入额(元)
            rqye: 当日融券余额(元)
            rqyl: 当日融券余量(股)
            rqmcl: 当日融券卖出量(股)
            rqchl: 当日融券偿还量(股)
            rqjmcl: 当日融券净卖出量(股)
            rzrqye: 当日融资融券余额(元)
            rzrqyecz: 当日融资融券余额差值(元)
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleMarginDetailsAllByDate(self._data, date, 1, Utility.random(8), retry, pause)
        self._data.rename(columns={'scode':'code', 'sname':'name'}, inplace=True)
        
        return self._result()
    
    
    def __handleMarginDetailsAllByDate(self, dataArr, date, page, randInt, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get(cf.MAR_BOTH_DETAIL % (date, page, randInt))
                text = request.text.split('=')[1]
                text = text.replace('{pages:', '{"pages":').replace(',data:', ',"data":').replace('"-"', '0')
                dataDict = Utility.str2Dict(text)
                data = dataDict['data']
                df = pd.DataFrame(data, columns=cf.MAR_DET_All_COLS)
                
                df['date'] = date
                df['rzyezb'] = df['rzyezb'].astype(float)
                    
                dataArr = dataArr.append(df, ignore_index=True)
                if page < dataDict['pages']:
                    dataArr = self.__handleMarginDetailsAllByDate(dataArr, date, page+1, randInt, retry, pause)
            except Exception as e:
                print(e)
            else:
                return dataArr
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def marginTotal(self, retry=3, pause=0.001):
        """
        两市合计融资融券历史数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'date':, 'close':, ...}, ...]
            date: 日期
            close: 沪深300收盘点数
            zdf: 沪深300收盘涨跌幅(%)
            rzye: 融资余额(元)
            rzyezb: 融资余额占比(%)
            rzmre: 融资买入额(元)
            rzche: 融资偿还额(元)
            rzjmre: 融资净买入额(元)
            rqye: 融券余额(元)
            rqyl: 融券余量(股)
            rqmcl: 融券卖出量(股)
            rqchl: 融券偿还量(股)
            rqjmcl: 融券净卖出量(股)
            rzrqye: 融资融券余额(元)
            rzrqyecz: 融资融券余额差值(元)
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleMarginTotal(self._data, 1, Utility.random(8), retry, pause)
        self._data.rename(columns={'tdate':'date'}, inplace=True)
        
        return self._result()
    
    
    def __handleMarginTotal(self, dataArr, page, randInt, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get(cf.MAR_TOTAL_URL % (page, randInt), timeout=10)
                text = request.text.split('=')[1]
                text = text.replace('{pages:', '{"pages":').replace(',data:', ',"data":').replace('T00:00:00', '').replace('"-"', '0')
                dataDict = Utility.str2Dict(text)
                data = dataDict['data']
                df = pd.DataFrame(data, columns=cf.MAR_TOTAL_COLS)
                
                df['close'] = df['close'].map(cf.FORMAT)
                df['rzyezb'] = df['rzyezb'].astype(float)
                
                dataArr = dataArr.append(df, ignore_index=True)
                if page < dataDict['pages']:
                    dataArr = self.__handleMarginTotal(dataArr, page+1, randInt, retry, pause)
            except Exception as e:
                print(e)
            else:
                return dataArr
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
