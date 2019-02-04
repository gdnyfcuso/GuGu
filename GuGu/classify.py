# -*- coding:utf-8 -*-
"""
股票所属类
Created on 2019/01/10
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""
import time
import json
import re
import pandas as pd
from base import Base, cf

class Classify(Base):        
    def byIndustry(self, std='sina', retry=3, pause=0.001):
        """
        获取行业分类数据
        Parameters
        ----------
        std : string
                sina:新浪行业 sw：申万 行业
        retry : int, 默认 10
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        
        Returns
        -------
        DataFrame or List: [{'code':, 'name':, ...}, ...]
            code :股票代码
            name :股票名称
            c_name :行业名称
        """
        self._data = pd.DataFrame()
        
        if std == 'sw':
            # http://vip.stock.finance.sina.com.cn/q/view/SwHy.php
            df = self.__getTypeData(cf.SINA_INDUSTRY_INDEX_URL % 'SwHy.php')
        else:
            # http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php
            df = self.__getTypeData(cf.SINA_INDUSTRY_INDEX_URL % 'newSinaHy.php')
            
        self._writeHead()
        for row in df.values:
            rowDf =  self.__getDetail(row[0], retry, pause)
            rowDf['c_name'] = row[1]
            self._data = self._data.append(rowDf, ignore_index=True)
        
        return self._result()
    
    
    def byConcept(self, retry=3, pause=0.001):
        """
        获取概念分类数据
        Parameters
        ----------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
        Return
        --------
        DataFrame
            code :股票代码
            name :股票名称
            c_name :概念名称
        """
        self._data = pd.DataFrame()
        
        self._writeHead()
        # http://money.finance.sina.com.cn/q/view/newFLJK.php?param=class
        df = self.__getTypeData( cf.SINA_CONCEPTS_INDEX_URL )
        
        for row in df.values:
            rowDf = self.__getDetail(row[0], retry, pause)
            rowDf['c_name'] = row[1]
            self._data = self._data.append(rowDf)
            
        return self._result()
    
            
    def __getTypeData(self, url):
        try:
            request = self._session.get(url, timeout=10)
            request.encoding = 'gbk'
            text = request.text.split('=')[1]
            dataJson = json.loads(text)
            df = pd.DataFrame([[row.split(',')[0], row.split(',')[1]] for row in dataJson.values()], columns=['tag', 'name'])
            
            return df
        except Exception as e:
            print(str(e))
    
    def __getDetail(self, tag, retry=3, pause=0.001):
        self._writeConsole()
        
        for retryCount in range(retry):
            time.sleep(pause)
            
            try:
                # http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=1000&sort=symbol&asc=1&node=new_zhhy&symbol=&_s_r_a=page
                request = self._session.get( cf.SINA_DATA_DETAIL_URL % tag, timeout=10 )
                reg = re.compile(r'\,(.*?)\:')
                text = reg.sub(r',"\1":', request.text)
                text = text.replace('"{symbol', '{"symbol')
                text = text.replace('{symbol', '{"symbol"')
                jstr = json.dumps(text)
                js = json.loads(jstr)
                
                df = pd.DataFrame(pd.read_json(js, dtype={'code':object}), columns=cf.FOR_CLASSIFY_B_COLS)
                
                return df
            except:
                time.sleep((retryCount+1)*10)
            
        raise IOError(cf.NETWORK_ERR_MSG)
    
    
