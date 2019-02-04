# -*- coding:utf-8 -*-
"""
基础类
Created on 2018/12/27
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""

import pandas as pd
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import config as cf


class Base():
    def __init__(self, pandas=True, inter=True):
        self.__pandas = False if not pandas else True
        self._PY3 = (sys.version_info[0] >= 3)
        self._inter = False if not inter else True      # 交互模式
        
        self._session = requests.Session()
        retry = Retry(connect=5, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        self._session.keep_alive = False
        
        self._data = pd.DataFrame()


    def _result(self):
        """
        返回结果：使用pandas时返回DataFrame否则返回list
        Parameters
        -----
            dataframe:pandas.DataFrame
            index:string
                    返回dict时选用的键 e.g. code
        return
        -----
            pandas.DataFrame or dict
        """
        if self._data.empty:
            return None
        elif self.__pandas:
            return self._data
        else:
            return self._data.to_dict('records')
        
        
    def output(self, full=False):
        print('')
        
        if not full:
            print(self._data)
        else:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('max_colwidth',100)
            
            print(self._data)
        
        
    def getPandas(self):
        return self.__pandas
    
    
    def setPandas(self, pandas=True):
        self.__pandas = pandas
        
        
    def getInter(self):
        """
        获取交互模式状态
        return
        ------
            True or False
        """
        return self._inter
    
    
    def setInter(self, inter=False):
        """
        设置交互模式
        Parameters
        ------
            inter: bool
        """
        self._inter = inter
        
        
    def _writeHead(self):
        if self._inter:
            sys.stdout.write(cf.GETTING_TIPS)
            sys.stdout.flush()


    def _writeConsole(self):
        if self._inter:
            sys.stdout.write(cf.GETTING_FLAG)
            sys.stdout.flush()  
        
        
