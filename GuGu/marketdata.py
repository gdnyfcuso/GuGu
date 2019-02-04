# -*- coding:utf-8 -*-
"""
市场类
Created on 2018/12/26
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""

import pandas as pd
from pandas.compat import StringIO
import re
import json
import numpy as np
from base import Base, cf


class MarketData(Base):
    def index(self):
        """
        获取大盘指数行情
        return
        -------
          DataFrame or list: [{'code':, 'name':, ...}, ...]
              code:指数代码
              name:指数名称
              change:涨跌幅
              open:开盘价
              preclose:昨日收盘价
              close:收盘价
              high:最高价
              low:最低价
              volume:成交量(手)
              amount:成交金额（亿元）
        """
        self._data = pd.DataFrame()
        
        # http://hq.sinajs.cn/rn=xppzh&list=sh000001,sh000002,sh000003,sh000008,sh000009,sh000010,sh000011,sh000012,sh000016,sh000017,sh000300,sh000905,sz399001,sz399002,sz399003,sz399004,sz399005,sz399006,sz399008,sz399100,sz399101,sz399106,sz399107,sz399108,sz399333,sz399606
        request = self._session.get( cf.INDEX_URL, timeout=10 )
        request.encoding = 'gbk'
        text = request.text.replace('var hq_str_', '')
        text = text.replace('";', '').replace('"', '').replace('=', ',')
        text = '%s%s'%(cf.INDEX_HEADER, text)
        
        self._data = pd.read_csv(StringIO(text), sep=',', thousands=',')
        self._data['change'] = (self._data['close'] / self._data['preclose'] - 1 ) * 100
        self._data['amount'] = self._data['amount'] / 100000000
        self._data['change'] = self._data['change'].map(cf.FORMAT)
        self._data['amount'] = self._data['amount'].map(cf.FORMAT4)
        self._data = self._data[cf.INDEX_COLS]
        self._data['code'] = self._data['code'].map(lambda x:str(x).zfill(6))
        self._data['change'] = self._data['change'].astype(float)
        self._data['amount'] = self._data['amount'].astype(float)
        
        return self._result()
    
    
    def latest(self):
        """
        一次性获取最近一个日交易日所有股票的交易数据
        return
        -------
          DataFrame or list: [{'code':, 'name':, ...}, ...]
               属性：代码，名称，涨跌幅，现价，开盘价，最高价，最低价，最日收盘价，成交量，换手率，成交额，市盈率，市净率，总市值，流通市值
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        
        self._data = self.__handleLatest(1)
        if self._data is not None:
            for i in range(2, cf.PAGE_NUM[0]):
                newData = self.__handleLatest(i)
                self._data = self._data.append(newData, ignore_index=True)
        
        return self._result()
    
    
    def __handleLatest(self, pageNum=1):
        """
        处理当日行情分页数据，格式为json
        Parameters
        ------
            pageNum:页码
        return
        -------
            DataFrame 当日所有股票交易数据(DataFrame)
        """
        self._writeConsole()
        # http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?num=80&sort=code&asc=0&node=hs_a&symbol=&_s_r_a=page&page=1
        request = self._session.get( cf.LATEST_URL % pageNum, timeout=10 )
        if self._PY3:
            request.encoding = 'gbk'
        text = request.text
        if text == 'null':
            return None
        
        reg = re.compile(r'\,(.*?)\:')
        text = reg.sub(r',"\1":', text)
        text = text.replace('"{symbol', '{"symbol')
        text = text.replace('{symbol', '{"symbol"')
        jstr = json.dumps(text)
        js = json.loads(jstr)
        df = pd.DataFrame(pd.read_json(js, dtype={'code':object}), columns=cf.DAY_TRADING_COLUMNS)
        df = df.drop('symbol', axis=1)

        return df
    
    
    def indexETF(self):
        """
        获取指数ETF及其相关数据
        return
        ------
        DataFrame or List: [{'fund_id':, 'fund_nm':, ...}, ...]
            fund_id:             基金代码
            fund_nm:             基金名称
            index_id:            跟踪指数代码
            creation_unit:       最小申赎单位（万份）
            amount:              份额（万份）
            unit_total:          规模（亿元）
            unit_incr:           规模变化（亿元）
            price:               现价
            volume:              成交额（万元）
            increase_rt:         涨幅（%）
            estimate_value:      估值
            discount_rt:         溢价率（%）
            fund_nav:            净值
            nav_dt:              净值日期
            index_nm:            指数名称
            index_increase_rt:   指数涨幅（%）
            pe:                  市盈率
            pb:                  市净率
        """
        self._data = pd.DataFrame()
        
        page = 1
        while(True):
            try:
                request = self._session.get( cf.INDEX_ETF_URL % page )
                text = request.text.replace('%', '')
                dataDict = json.loads(text)
                if dataDict['page'] < page:
                    break
                
                dataList = []
                for row in dataDict['rows']:
                    dataList.append(row['cell'])            
                self._data = self._data.append(pd.DataFrame(dataList, columns = cf.INDEX_ETF_COLS), ignore_index=True)
                
                page += 1
            except Exception as e:
                print(str(e))
        
        self._data[self._data=="-"] = np.NaN
        for col in ['creation_unit', 'amount', 'unit_total', 'unit_incr', 'price', 'volume', 'increase_rt',
                    'estimate_value', 'discount_rt', 'fund_nav', 'index_increase_rt', 'pe', 'pb']:
            self._data[col] = self._data[col].astype(float)

        self._data['index_id'] = self._data['index_id'].map(lambda x : x if x != "0" else None)        
            
        return self._result()
    
    
