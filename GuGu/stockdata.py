# -*- coding:utf-8 -*-
"""
股票交易类
Created on 2018/12/26
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""
from __future__ import division

import time
import json
import re
import lxml.html
from lxml import etree
import pandas as pd
from pandas.compat import StringIO
from base import Base, cf
from utility import Utility

class StockData(Base):
    def __init__(self, code=None, pandas=True, inter=True):
        Base.__init__(self, pandas, inter)
        self.__code = code
        
    
    def history(self, start='', end='', ktype='D', autype='qfq', index=False, retry=3, pause=0.001):
        """
        获取股票交易历史数据
        ---------
        Parameters:
          start:string
                      开始日期 format：YYYY-MM-DD 为空时取上市首日
          end:string
                      结束日期 format：YYYY-MM-DD 为空时取最近一个交易日
          autype:string
                      复权类型，qfq-前复权 hfq-后复权 None-不复权，默认为qfq
          ktype：string
                      数据类型，D=日k线 W=周 M=月 5=5分钟 15=15分钟 30=30分钟 60=60分钟，默认为D
          retry: int, 默认 3
                     如遇网络等问题重复执行的次数 
          pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        return
        -------
          DataFrame or list: [{'date':, 'open':, ...}, ...]
              date 交易日期 (index)
              open 开盘价
              high  最高价
              close 收盘价
              low 最低价
              volume 成交量
              code 股票代码
        """
        self._data = pd.DataFrame()
        
        url = ''
        dataflag = ''
        symbol = cf.INDEX_SYMBOL[self.__code] if index else Utility.symbol(self.__code)
        autype = '' if autype is None else autype
        
        if (start is not None) & (start != ''):
            end = Utility.getToday() if end is None or end == '' else end
            
        if ktype.upper() in cf.K_LABELS:
            fq = autype if autype is not None else ''
            if self.__code[:1] in ('1', '5') or index:
                fq = ''
                
            kline = '' if autype is None else 'fq'
            if (start is None or start == '') & (end is None or end == ''):
                # http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq&param=sh600000,day,,,640,qfq&r=0.73327461855388564
                urls = [cf.HISTORY_URL % (kline, fq, symbol, cf.TT_K_TYPE[ktype.upper()], start, end, fq, Utility.random(17))]
            else:
                years = Utility.ttDates(start, end)
                urls = []
                for year in years:
                    startdate = str(year) + '-01-01'
                    enddate = str(year+1) + '-12-31'
                    # http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq2008&param=sh600000,day,2008-01-01,2009-12-31,640,qfq&r=0.73327461855388564
                    url = cf.HISTORY_URL % (kline, fq+str(year), symbol, cf.TT_K_TYPE[ktype.upper()], startdate, enddate, fq, Utility.random(17))
                    urls.append(url)
            dataflag = '%s%s'%(fq, cf.TT_K_TYPE[ktype.upper()])     # qfqday
        elif ktype in cf.K_MIN_LABELS:
            # http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=sh600000,m30,,640&_var=m30_today&r=0.5537154641907898
            urls = [cf.HISTORY_MIN_URL % (symbol, ktype, ktype, Utility.random(16))]
            dataflag = 'm%s'%ktype      # m30
        else:
            raise TypeError('ktype input error.')
        
        for url in urls:
            self._data = self._data.append(self.__handleHistory(url, dataflag, symbol, index, ktype, retry, pause), ignore_index=True)
        if ktype not in cf.K_MIN_LABELS:
            if ((start is not None) & (start != '')) & ((end is not None) & (end != '')):
                self._data = self._data[(self._data.date >= start) & (self._data.date <= end)]
        
        return self._result()
    
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def __handleHistory(self, url, dataflag='', symbol='', index = False, ktype = '', retry=3, pause=0.001):
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get(url, timeout=10)
                if self._PY3:
                    request.encoding = 'gbk'
                lines = request.text
                if len(lines) < 100: #no data
                    return None
            except Exception as e:
                print(e)
            else:
                lines = lines.split('=')[1]
                reg = re.compile(r',{"nd.*?}') 
                lines = re.subn(reg, '', lines) 
                js = json.loads(lines[0])
                dataflag = dataflag if dataflag in list(js['data'][symbol].keys()) else cf.TT_K_TYPE[ktype.upper()]
                
                if ktype in cf.K_MIN_LABELS:
                    for value in js['data'][symbol][dataflag]:
                        value.pop()
                        value.pop()
                        
                df = pd.DataFrame(js['data'][symbol][dataflag], columns=cf.KLINE_TT_COLS)
                df['code'] = symbol if index else self.__code
                if ktype in cf.K_MIN_LABELS:
                    df['date'] = df['date'].map(lambda x: '%s-%s-%s %s:%s'%(x[0:4], x[4:6], x[6:8], x[8:10], x[10:12]))
                for col in df.columns[1:6]:
                    df[col] = df[col].astype(float)
                    
                return df
            
            
    def xrxd(self, date='', retry=3, pause=0.001):
        """
        获取股票除权除息信息
        Parameters
        ------
            date: string
                format：YYYY-MM-DD
            retry: int, 默认 3
                如遇网络等问题重复执行的次数 
            pause : int, 默认 0
                重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                
        return
        ------
            Dict or None
                nd, 对应年份
                fh_sh, 分红数额
                djr, 股权登记日
                cqr, 除权日
                FHcontent, 除权除息信息
        """
        data = None
        
        if not date:
            date = Utility.getToday()
            
        symbol = Utility.symbol(self.__code)
        
        for _ in range(retry):
            time.sleep(pause)
            
            url = cf.HISTORY_URL % ('fq', 'qfq', symbol, 'day', date, date, 'qfq', Utility.random(17))
            try:
                request = self._session.get(url, timeout=10)
                pattern = re.compile(r'({"nd".+?})')
                result = re.search(pattern, request.text)
                if result:
                    data = eval(result.group(1))
            except Exception as e:
                print(str(e))
                
            return data
                
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def realtime(self):
        """
        获取实时交易数据 getting real time quotes data
        用于跟踪交易情况（本次执行的结果-上一次执行的数据）
        Parameters
        ------
            stockdata(code) code : string, array-like object (list, tuple, Series).
        return
        -------
            DataFrame 实时交易数据 or list: [{'name':, 'open':, ...}, ...]
            属性:0：name，股票名字
                1：open，今日开盘价
                2：pre_close，昨日收盘价
                3：price，当前价格
                4：high，今日最高价
                5：low，今日最低价
                6：bid，竞买价，即“买一”报价
                7：ask，竞卖价，即“卖一”报价
                8：volumn，成交量 maybe you need do volumn/100
                9：amount，成交金额（元 CNY）
                10：b1_v，委买一（笔数 bid volume）
                11：b1_p，委买一（价格 bid price）
                12：b2_v，“买二”
                13：b2_p，“买二”
                14：b3_v，“买三”
                15：b3_p，“买三”
                16：b4_v，“买四”
                17：b4_p，“买四”
                18：b5_v，“买五”
                19：b5_p，“买五”
                20：a1_v，委卖一（笔数 ask volume）
                21：a1_p，委卖一（价格 ask price）
                ...
                30：date，日期；
                31：time，时间；
        """
        self._data = pd.DataFrame()
        
        symbols_list = ''
        if isinstance(self.__code, list) or isinstance(self.__code, set) or isinstance(self.__code, tuple) or isinstance(self.__code, pd.Series):
            for code in self.__code:
                symbols_list += Utility.symbol(code) + ','
        else:
            symbols_list = Utility.symbol(self.__code)
        symbols_list = symbols_list[:-1] if len(symbols_list) > 8 else symbols_list
        
        # http://hq.sinajs.cn/rn=4879967949085&list=sh600000,sh600004
        request = self._session.get( cf.LIVE_DATA_URL % (Utility.random(), symbols_list), timeout=10 )
        request.encoding = 'gbk'
        reg = re.compile(r'\="(.*?)\";')
        data = reg.findall(request.text)
        regSym = re.compile(r'(?:sh|sz)(.*?)\=')
        syms = regSym.findall(request.text)
        data_list = []
        syms_list = []
        for index, row in enumerate(data):
            if len(row)>1:
                data_list.append([astr for astr in row.split(',')])
                syms_list.append(syms[index])
        if len(syms_list) == 0:
            return None
        
        self._data = pd.DataFrame(data_list, columns=cf.LIVE_DATA_COLS)
        self._data = self._data.drop('s', axis=1)
        self._data['code'] = syms_list
        ls = [cls for cls in self._data.columns if '_v' in cls]
        for txt in ls:
            self._data[txt] = self._data[txt].map(lambda x : x[:-2])
            
        return self._result()
    
    
    def historyTicks(self, date=None, retry=3, pause=0.001):
        """
        获取历史分笔明细数据
        Parameters
        ------
            date : string
                    日期 format：YYYY-MM-DD, 默认为前一个交易日
            retry : int, 默认 3
                    如遇网络等问题重复执行的次数
            pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
         return
         -------
            DataFrame 当日所有股票交易数据(DataFrame) or list: [{'time':, 'price':, ...}, ...]
                  属性:成交时间、成交价格、价格变动，成交手、成交金额(元)，买卖类型
        """
        self._data = pd.DataFrame()
        
        symbol = Utility.symbol(self.__code)
        date = Utility.lastTradeDate() if date is None else date
            
        try:
            self._writeHead()
            
            page = 1
            while(True):
                # http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol=sh600000&date=2018-12-26&page=1
                # http://market.finance.sina.com.cn/transHis.php?date=2019-01-25&symbol=sh600000&page=1
                url = cf.HISTORY_TICKS_URL % (date, symbol, page)
                tick_data = self.__handleTicks(url, cf.HISTORY_TICK_COLUMNS, retry, pause)
                if tick_data is not None:
                    self._data = self._data.append(tick_data, ignore_index=True)
                    page = page + 1
                else:
                    break
        except Exception as er:
            print(str(er))
        else:
            return self._result()
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
        
        
    def __handleTicks(self, url, column, retry, pause):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                html = lxml.html.parse(url)
                res = html.xpath('//table[@id=\"datatbl\"]/tbody/tr')
                if not res:
                    return None
                
                if self._PY3:
                    sarr = [etree.tostring(node).decode('utf-8') for node in res]
                else:
                    sarr = [etree.tostring(node) for node in res]
                sarr = ''.join(sarr)
                sarr = '<table>%s</table>'%sarr
                sarr = sarr.replace('--', '0')
                df = pd.read_html(StringIO(sarr), parse_dates=False)
                df = pd.read_html(StringIO(sarr), parse_dates=False)[0]
                df.columns = column
                if 'pchange' in column:
                    df['pchange'] = df['pchange'].map(lambda x : x.replace('%', ''))
            except Exception as e:
                print(e)
            else:
                return df
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    def todayTicks(self, retry=3, pause=0.001):
        """
        获取当日分笔明细数据
        Parameters
        ------
            retry : int, 默认 3
                      如遇网络等问题重复执行的次数
            pause : int, 默认 0
                     重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
         return
         -------
            DataFrame 当日所有股票交易数据(DataFrame) or list: [{'time':, 'price':, ...}, ...]
                  属性:成交时间、成交价格、涨跌幅、价格变动，成交手、成交金额(元)，买卖类型
        """
        self._data = pd.DataFrame()
        
        if self.__code is None or len(self.__code)!=6 :
            return None
        
        if not Utility.isTradeDay():
            return None
        
        # 不到交易时间
        openTime = time.mktime(time.strptime(Utility.getToday() + ' 09:25:00', '%Y-%m-%d %H:%M:%S'))
        now = time.time()
        if now < openTime:
            return None
        
        symbol = Utility.symbol(self.__code)
        date = Utility.getToday()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                # http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Transactions.getAllPageTime?date=2018-12-26&symbol=sh600000
                request = self._session.get( cf.TODAY_TICKS_PAGE_URL % (date, symbol), timeout=10 )
                request.encoding = 'gbk'
                text = request.text[1:-1]
                data_dict = Utility.str2Dict(text)
                pages = len(data_dict['detailPages'])
                
                self._writeHead()
                for pNo in range(1, pages+1):
                    # http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=sh600000&date=2018-12-26&page=1
                    url = cf.TODAY_TICKS_URL % (symbol, date, pNo)
                    self._data = self._data.append(self.__handleTicks(url, cf.TODAY_TICK_COLUMNS, retry, pause), ignore_index=True)
            except Exception as e:
                print(str(e))
            else:
                return self._result()
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def bigDeal(self, date=None, vol=400, retry=3, pause=0.001):
        """
        获取大单数据
        Parameters
        ------
            date:string
                      日期 format：YYYY-MM-DD
            retry : int, 默认 3
                      如遇网络等问题重复执行的次数
            pause : int, 默认 0
                     重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
         return
         -------
            DataFrame 当日所有股票交易数据(DataFrame) or list: [{'code':, 'name', ...}, ...]
                  属性:股票代码    股票名称    交易时间    价格    成交量    前一笔价格    类型（买、卖、中性盘）
        """
        self._data = pd.DataFrame()
        
        if self.__code is None or len(self.__code) != 6 or date is None:
            return None
        
        symbol = Utility.symbol(self.__code)
        vol = vol*100
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                # http://vip.stock.finance.sina.com.cn/quotes_service/view/cn_bill_download.php?symbol=sh600000&num=60&page=1&sort=ticktime&asc=0&volume=40000&amount=0&type=0&day=2018-12-26
                request = self._session.get( cf.SINA_DD % (symbol, vol, date), timeout=10 )
                request.encoding = 'gbk'
                lines = request.text
                if len(lines) < 100:
                    return None
                self._data = pd.read_csv(StringIO(lines), names=cf.SINA_DD_COLS, skiprows=[0])    
                if self._data is not None:
                    self._data['code'] = self._data['code'].map(lambda x: x[2:])
            except Exception as e:
                print(e)
            else:
                return self._result()
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
