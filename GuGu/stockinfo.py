# -*- coding:utf-8 -*-
"""
股票信息类
Created on 2019/01/02
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""
from __future__ import division

import time
import pandas as pd
from pandas.compat import StringIO
import lxml.html
from lxml import etree
import re
from utility import Utility
from base import Base, cf

class StockInfo(Base):
    def stockProfiles(self, retry=3, pause=0.001):
        """
        获取上市公司基本情况
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
                   code,代码
                   name,名称
                   city,所在城市
                   staff,员工人数
                   date,上市日期
                   industry,行业分类
                   pro_type,产品类型
                   main,主营业务
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        date = '%s-12-31' % Utility.getYear()
        self._data = pd.DataFrame()
        
        self._data = self.__handleStockProfiles(self._data, date, 1, retry, pause)
        
        return self._result()
    
    
    def __handleStockProfiles(self, dataArr, date, page, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                html = lxml.html.parse(cf.ALL_STOCK_PROFILES_URL % (date, page))
                res = html.xpath('//table[@id="myTable04"]/tbody/tr')
                if not res:
                    return dataArr
                
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = '<table>%s</table>' % sarr
                
                df = pd.read_html(sarr)[0]
                df = df.drop([0, 3, 5, 6, 7, 10, 11], axis = 1)
                df.columns = cf.ALL_STOCK_PROFILES_COLS
                df['code'] = df['code'].map(lambda x: str(x).zfill(6))
                
                dataArr = dataArr.append(df, ignore_index=True)
            except Exception as e:
                print(e)
            else:
                return self.__handleStockProfiles(dataArr, date, page+1, retry, pause)
        
    
    def report(self, year, quarter, retry=3, pause=0.001):
        """
        获取业绩报表数据
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
            eps,每股收益
            eps_yoy,每股收益同比(%)
            bvps,每股净资产
            roe,净资产收益率(%)
            epcf,每股现金流量(元)
            net_profits,净利润(万元)
            profits_yoy,净利润同比(%)
            distrib,分配方案
            report_date,发布日期
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/mainindex/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.REPORT_URL, year, quarter, 1, cf.REPORT_COLS, pd.DataFrame(), retry, pause, 11)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x:str(x).zfill(6))
                
            return self._result()
        
    def profit(self, year, quarter, retry=3, pause=0.001):
        """
        获取盈利能力数据
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
            roe,净资产收益率(%)
            net_profit_ratio,净利率(%)
            gross_profit_rate,毛利率(%)
            net_profits,净利润(万元)
            eps,每股收益
            business_income,营业收入(百万元)
            bips,每股主营业务收入(元)
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/profit/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.PROFIT_URL, year, quarter, 1, cf.PROFIT_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def operation(self, year, quarter, retry=3, pause=0.001):
        """
        获取营运能力数据
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
            arturnover,应收账款周转率(次)
            arturndays,应收账款周转天数(天)
            inventory_turnover,存货周转率(次)
            inventory_days,存货周转天数(天)
            currentasset_turnover,流动资产周转率(次)
            currentasset_days,流动资产周转天数(天)
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/operation/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.OPERATION_URL, year, quarter, 1, cf.OPERATION_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def growth(self, year, quarter, retry=3, pause=0.001):
        """
        获取成长能力数据
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
            mbrg,主营业务收入增长率(%)
            nprg,净利润增长率(%)
            nav,净资产增长率
            targ,总资产增长率
            epsg,每股收益增长率
            seg,股东权益增长率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/grow/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.GROWTH_URL, year, quarter, 1, cf.GROWTH_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                 
            return self._result()
        
    def debtPaying(self, year, quarter, retry=3, pause=0.001):
        """
        获取偿债能力数据
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
            currentratio,流动比率
            quickratio,速动比率
            cashratio,现金比率
            icratio,利息支付倍数
            sheqratio,股东权益比率
            adratio,股东权益增长率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/debtpaying/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.DEBTPAYING_URL, year, quarter, 1, cf.DEBTPAYING_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def cashFlow(self, year, quarter, retry=3, pause=0.001):
        """
        获取现金流量数据
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
            cf_sales,经营现金净流量对销售收入比率
            rateofreturn,资产的经营现金流量回报率
            cf_nm,经营现金净流量与净利润的比率
            cf_liabilities,经营现金净流量对负债比率
            cashflowratio,现金流量比率
        """
        self._data = pd.DataFrame()
        
        if Utility.checkQuarter(year, quarter) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/cashflow/index.phtml?s_i=&s_a=&s_c=&reportdate=2018&quarter=3&p=1&num=60
            self._data = self.__parsePage(cf.CASHFLOW_URL, year, quarter, 1, cf.CASHFLOW_COLS, pd.DataFrame(), retry, pause)
            if self._data is not None:
                self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
                
            return self._result()
        
    def __parsePage(self, url, year, quarter, page, column, dataArr, retry, pause, drop_column=None):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get( url % (year, quarter, page, cf.PAGE_NUM[1]), timeout=10 )
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
                if drop_column is not None:
                    df = df.drop(drop_column, axis=1)
                df.columns = column
                dataArr = dataArr.append(df, ignore_index=True)
                nextPage = html.xpath('//div[@class=\"pages\"]/a[last()]/@onclick')
                if len(nextPage) > 0:
                    page = re.findall(r'\d+', nextPage[0])[0]
                    return self.__parsePage(url, year, quarter, page, column, dataArr, retry, pause, drop_column)
                else:
                    return dataArr
            except Exception as e:
                print(e)
                
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
