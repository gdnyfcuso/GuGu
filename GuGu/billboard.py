# -*- coding:utf-8 -*-
"""
龙虎榜类
Created on 2019/01/11
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""

import time
import re
import pandas as pd
from pandas.compat import StringIO
import lxml.html
from lxml import etree
from base import Base, cf
from utility import Utility

class BillBoard(Base):
    def topList(self, date = None, retry=3, pause=0.001):
        """
        获取每日龙虎榜列表
        Parameters
        --------
        date:string
                    明细数据日期 format：YYYY-MM-DD 如果为空，返回最近一个交易日的数据
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code：代码
            name ：名称
            pchange：涨跌幅     
            amount：龙虎榜成交额(万)
            buy：买入额(万)
            bratio：占总成交比例
            sell：卖出额(万)
            sratio ：占总成交比例
            reason：上榜原因
            unscramble: 解读
            date  ：日期
        """
        self._data = pd.DataFrame()
        
        if date is None:
            if Utility.getHour() < 18:
                date = Utility.lastTradeDate()
            else:
                date = Utility.getToday()
        else:
            if not Utility.isTradeDay(date):
                return None
            
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                # http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=2019-01-10,endDate=2019-01-10,gpfw=0,js=vardata_tab_1.html
                request = self._session.get( cf.LHB_URL % (date, date), timeout=10 )
                request.encoding = 'gbk'
                text = request.text.split('_1=')[1]
                dataDict = Utility.str2Dict(text)
                
                self._data = pd.DataFrame(dataDict['data'], columns=cf.LHB_TMP_COLS)
                self._data.columns = cf.LHB_COLS
                self._data['buy'] = self._data['buy'].astype(float)
                self._data['sell'] = self._data['sell'].astype(float)
                self._data['amount'] = self._data['amount'].astype(float)
                self._data['Turnover'] = self._data['Turnover'].astype(float)
                self._data['bratio'] = self._data['buy'] / self._data['Turnover']
                self._data['sratio'] = self._data['sell'] / self._data['Turnover']
                self._data['bratio'] = self._data['bratio'].map(cf.FORMAT)
                self._data['sratio'] = self._data['sratio'].map(cf.FORMAT)
                self._data['date'] = date
                for col in ['amount', 'buy', 'sell']:
                    self._data[col] = self._data[col].astype(float)
                    self._data[col] = self._data[col] / 10000
                    self._data[col] = self._data[col].map(cf.FORMAT)
                self._data = self._data.drop('Turnover', axis=1)
            except:
                pass
            else:
                return self._result()
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def countTops(self, days=5, retry=3, pause=0.001):
        """
        获取个股上榜统计数据
        Parameters
        --------
            days:int
                      天数，统计n天以来上榜次数，默认为5天，其余是10、30、60
            retry : int, 默认 3
                         如遇网络等问题重复执行的次数 
            pause : int, 默认 0
                        重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        Return
        ------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code：代码
            name：名称
            count：上榜次数
            bamount：累积购买额(万)     
            samount：累积卖出额(万)
            net：净额(万)
            bcount：买入席位数
            scount：卖出席位数
        """
        self._data = pd.DataFrame()
        
        if Utility.checkLhbInput(days) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/ggtj/index.phtml?last=5&p=1
            self._data =  self.__parsePage(kind=cf.LHB_KINDS[0], last=days, column=cf.LHB_GGTJ_COLS, dataArr=pd.DataFrame(), pageNo=1, retry=retry, pause=pause)
            self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
            if self._data is not None:
                self._data = self._data.drop_duplicates('code')
                
            return self._result()
        
        
    def brokerTops(self, days=5, retry=3, pause=0.001):
        """
        获取营业部上榜统计数据
        Parameters
        --------
        days:int
                  天数，统计n天以来上榜次数，默认为5天，其余是10、30、60
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        Return
        ---------
        DataFrame or List: [{'broker':, 'count':, ...}, ...]
            broker：营业部名称
            count：上榜次数
            bamount：累积购买额(万)
            bcount：买入席位数
            samount：累积卖出额(万)
            scount：卖出席位数
            top3：买入前三股票
        """
        self._data = pd.DataFrame()
        
        if Utility.checkLhbInput(days) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/yytj/index.phtml?last=5&p=1
            self._data = self.__parsePage(kind=cf.LHB_KINDS[1], last=days, column=cf.LHB_YYTJ_COLS, dataArr=pd.DataFrame(), pageNo=1, retry=retry, pause=pause)
            
            return self._result()
        
        
    def instTops(self, days=5, retry=3, pause=0.001):
        """
        获取机构席位追踪统计数据
        Parameters
        --------
        days:int
                  天数，统计n天以来上榜次数，默认为5天，其余是10、30、60
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code:代码
            name:名称
            bamount:累积买入额(万)
            bcount:买入次数
            samount:累积卖出额(万)
            scount:卖出次数
            net:净额(万)
        """
        self._data = pd.DataFrame()
        
        if Utility.checkLhbInput(days) is True:
            self._writeHead()
            
            # http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/jgzz/index.phtml?last=5&p=1
            self._data = self.__parsePage(kind=cf.LHB_KINDS[2], last=days, column=cf.LHB_JGZZ_COLS, dataArr=pd.DataFrame(), pageNo=1, retry=retry, pause=pause, drop_column=[2,3])
            self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
            
            return self._result()
        
        
    def instDetail(self, retry=3, pause=0.001):
        """
        获取最近一个交易日机构席位成交明细统计数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        ----------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code:股票代码
            name:股票名称     
            date:交易日期     
            bamount:机构席位买入额(万)     
            samount:机构席位卖出额(万)     
            type:类型
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        # http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/jgmx/index.phtml?last=&p=1
        self._data = self.__parsePage(kind=cf.LHB_KINDS[3], last='', column=cf.LHB_JGMX_COLS, dataArr=pd.DataFrame(), pageNo=1, retry=retry, pause=pause)
        if len(self._data) > 0:
            self._data['code'] = self._data['code'].map(lambda x: str(x).zfill(6))
            
        return self._result()
        
    
    def __parsePage(self, kind, last, column, dataArr, pageNo=1, retry=3, pause=0.001, drop_column=None):
        self._writeConsole()
        
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                request = self._session.get( cf.LHB_SINA_URL % (kind, last, pageNo), timeout=10 )
                request.encoding = 'gbk'
                html = lxml.html.parse(StringIO(request.text))
                res = html.xpath("//table[@id=\"dataTable\"]/tr")
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
                    pageNo = re.findall(r'\d+', nextPage[0])[0]
                    return self.__parsePage(kind, last, column, dataArr, pageNo, retry, pause, drop_column)
                else:
                    return dataArr
            except Exception as e:
                print(e)
                
                
