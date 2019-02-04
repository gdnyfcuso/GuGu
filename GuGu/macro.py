# -*- coding:utf-8 -*-
"""
宏观经济数据类
Created on 2019/01/09
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""
import pandas as pd
import numpy as np
import re
import json
import time
from utility import Utility
from base import Base, cf


class Macro(Base):
    def gdpYear(self, retry=3, pause=0.001):
        """
        获取年度国内生产总值数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'year':, 'gdp':, ...}, ...]
            year :统计年度
            gdp :国内生产总值(亿元)
            pc_gdp :人均国内生产总值(元)
            gnp :国民生产总值(亿元)
            pi :第一产业(亿元)
            si :第二产业(亿元)
            industry :工业(亿元)
            cons_industry :建筑业(亿元)
            ti :第三产业(亿元)
            trans_industry :交通运输仓储邮电通信业(亿元)
            lbdy :批发零售贸易及餐饮业(亿元)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK4224641560861/MacPage_Service.get_pagedata?cate=nation&event=0&from=0&num=70&condition=&_=4224641560861
        datastr = self.__parsePage('nation', 0, 70, retry, pause)
        datastr = datastr.replace('"', '').replace('null', '0')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.GDP_YEAR_COLS)
        self._data[self._data==0] = np.NaN
        
        return self._result()
    
    def gdpQuarter(self, retry=3, pause=0.001):
        """
        获取季度国内生产总值数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'quarter':, 'gdp':, ...}, ...]
            quarter :季度
            gdp :国内生产总值(亿元)
            gdp_yoy :国内生产总值同比增长(%)
            pi :第一产业增加值(亿元)
            pi_yoy:第一产业增加值同比增长(%)
            si :第二产业增加值(亿元)
            si_yoy :第二产业增加值同比增长(%)
            ti :第三产业增加值(亿元)
            ti_yoy :第三产业增加值同比增长(%)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK3935140379887/MacPage_Service.get_pagedata?cate=nation&event=1&from=0&num=250&condition=&_=3935140379887
        datastr = self.__parsePage('nation', 1, 250, retry, pause)
        datastr = datastr.replace('"', '').replace('null', '0')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.GDP_QUARTER_COLS)
        self._data['quarter'] = self._data['quarter'].astype(object)
        self._data[self._data==0] = np.NaN
        
        return self._result()
    
    def demandsToGdp(self, retry=3, pause=0.001):
        """
        获取三大需求对GDP贡献数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'year':, 'cons_to':, ...}, ...]
            year :统计年度
            cons_to :最终消费支出贡献率(%)
            cons_rate :最终消费支出拉动(百分点)
            asset_to :资本形成总额贡献率(%)
            asset_rate:资本形成总额拉动(百分点)
            goods_to :货物和服务净出口贡献率(%)
            goods_rate :货物和服务净出口拉动(百分点)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK3153587567694/MacPage_Service.get_pagedata?cate=nation&event=4&from=0&num=80&condition=&_=3153587567694
        datastr = self.__parsePage('nation', 4, 80, retry, pause)
        datastr = datastr.replace('"','').replace('null','0')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js,columns=cf.GDP_FOR_COLS)
        self._data[self._data==0] = np.NaN
        
        return self._result()
        
    
    def idsPullToGdp(self, retry=3, pause=0.001):
        """
        获取三大产业对GDP拉动数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'year':, 'gdp_yoy':, ...}, ...]
            year :统计年度
            gdp_yoy :国内生产总值同比增长(%)
            pi :第一产业拉动率(%)
            si :第二产业拉动率(%)
            industry:其中工业拉动(%)
            ti :第三产业拉动率(%)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK1083239038283/MacPage_Service.get_pagedata?cate=nation&event=5&from=0&num=60&condition=&_=1083239038283
        datastr = self.__parsePage('nation', 5, 60, retry, pause)
        datastr = datastr.replace('"', '').replace('null', '0')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.GDP_PULL_COLS)
        self._data[self._data==0] = np.NaN
        
        return self._result()
    
    
    def idsCtbToGdp(self, retry=3, pause=0.001):
        """
        获取三大产业贡献率数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'year':, 'gdp_yoy':, ...}, ...]
            year :统计年度
            gdp_yoy :国内生产总值
            pi :第一产业献率(%)
            si :第二产业献率(%)
            industry:其中工业献率(%)
            ti :第三产业献率(%)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK4658347026358/MacPage_Service.get_pagedata?cate=nation&event=6&from=0&num=60&condition=&_=4658347026358
        datastr = self.__parsePage('nation', 6, 60, retry, pause)
        datastr = datastr.replace('"', '').replace('null', '0')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.GDP_CONTRIB_COLS)
        self._data[self._data==0] = np.NaN
        
        return self._result()
    
    
    def cpi(self, retry=3, pause=0.001):
        """
        获取居民消费价格指数数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'month':, 'cpi':,}, ...]
            month :统计月份
            cpi :价格指数
        """
        self._data = pd.DataFrame()
        
        datastr = self.__parsePage('price', 0, 600, retry, pause)
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.CPI_COLS)
        self._data['cpi'] = self._data['cpi'].astype(float)
        
        return self._result()
    
    
    def ppi(self, retry=3, pause=0.001):
        """
        获取工业品出厂价格指数数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'month':, 'ppiip':, ...}, ...]
            month :统计月份
            ppiip :工业品出厂价格指数
            ppi :生产资料价格指数
            qm:采掘工业价格指数
            rmi:原材料工业价格指数
            pi:加工工业价格指数    
            cg:生活资料价格指数
            food:食品类价格指数
            clothing:衣着类价格指数
            roeu:一般日用品价格指数
            dcg:耐用消费品价格指数
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK6734345383111/MacPage_Service.get_pagedata?cate=price&event=3&from=0&num=600&condition=&_=6734345383111
        datastr = self.__parsePage('price', 3, 600, retry, pause)
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.PPI_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, np.NaN, x))
            if i != 'month':
                self._data[i] = self._data[i].astype(float)
                
        return self._result()
    
    
    def depositRate(self, retry=3, pause=0.001):
        """
        获取存款利率数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'date':, 'deposit_type':, ...}, ...]
            date :变动日期
            deposit_type :存款种类
            rate:利率（%）
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK1250640915421/MacPage_Service.get_pagedata?cate=fininfo&event=2&from=0&num=600&condition=&_=1250640915421
        datastr = self.__parsePage('fininfo', 2, 600, retry, pause)
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.DEPOSIT_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, '--', x))
            
        return self._result()
    
    
    def loanRate(self, retry=3, pause=0.001):
        """
        获取贷款利率数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'date':, 'loan_type':, ...}, ...]
            date :执行日期
            loan_type :存款种类
            rate:利率（%）
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK7542659823280/MacPage_Service.get_pagedata?cate=fininfo&event=3&from=0&num=800&condition=&_=7542659823280
        datastr = self.__parsePage('fininfo', 3, 800, retry, pause)
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.LOAN_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, '--', x))
            
        return self._result()
    
    
    def rrr(self, retry=3, pause=0.001):
        """
        获取存款准备金率数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'date':, 'before':, ...}, ...]
            date :变动日期
            before :调整前存款准备金率(%)
            now:调整后存款准备金率(%)
            changed:调整幅度(%)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK8028217046046/MacPage_Service.get_pagedata?cate=fininfo&event=4&from=0&num=100&condition=&_=8028217046046
        datastr = self.__parsePage('fininfo', 4, 100, retry, pause)
        datastr = datastr if self._PY3 else datastr.decode('gbk')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.RRR_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, '--', x))
            
        return self._result()
    
    
    def montySupply(self, retry=3, pause=0.001):
        """
        获取货币供应量数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'month':, 'm2':, ...}, ...]
            month :统计时间
            m2 :货币和准货币（广义货币M2）(亿元)
            m2_yoy:货币和准货币（广义货币M2）同比增长(%)
            m1:货币(狭义货币M1)(亿元)
            m1_yoy:货币(狭义货币M1)同比增长(%)
            m0:流通中现金(M0)(亿元)
            m0_yoy:流通中现金(M0)同比增长(%)
            cd:活期存款(亿元)
            cd_yoy:活期存款同比增长(%)
            qm:准货币(亿元)
            qm_yoy:准货币同比增长(%)
            ftd:定期存款(亿元)
            ftd_yoy:定期存款同比增长(%)
            sd:储蓄存款(亿元)
            sd_yoy:储蓄存款同比增长(%)
            rests:其他存款(亿元)
            rests_yoy:其他存款同比增长(%)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK9019314616219/MacPage_Service.get_pagedata?cate=fininfo&event=1&from=0&num=600&condition=&_=9019314616219
        datastr = self.__parsePage('fininfo', 1, 600, retry, pause)
        datastr = datastr if self._PY3 else datastr.decode('gbk')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.MONEY_SUPPLY_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, '--', x))
            
        return self._result()
    
    
    def moneySupplyBal(self, retry=3, pause=0.001):
        """
        获取货币供应量(年底余额)数据
        Parameters
        --------
        retry : int, 默认 3
                     如遇网络等问题重复执行的次数 
        pause : int, 默认 0.001
                    重复请求数据过程中暂停的秒数，防止请求间隔时间太短出现的问题
                    
        Return
        --------
        DataFrame or List: [{'year':, 'm2':, ...}, ...]
            year :统计年度
            m2 :货币和准货币(亿元)
            m1:货币(亿元)
            m0:流通中现金(亿元)
            cd:活期存款(亿元)
            qm:准货币(亿元)
            ftd:定期存款(亿元)
            sd:储蓄存款(亿元)
            rests:其他存款(亿元)
        """
        self._data = pd.DataFrame()
        
        # http://money.finance.sina.com.cn/mac/api/jsonp.php/SINAREMOTECALLCALLBACK3430820865181/MacPage_Service.get_pagedata?cate=fininfo&event=0&from=0&num=200&condition=&_=3430820865181
        datastr = self.__parsePage('fininfo', 0, 200, retry, pause)
        datastr = datastr if self._PY3 else datastr.decode('gbk')
        js = json.loads(datastr)
        self._data = pd.DataFrame(js, columns=cf.MONEY_SUPPLY_BLA_COLS)
        for i in self._data.columns:
            self._data[i] = self._data[i].apply(lambda x:np.where(x is None, '--', x))
            
        return self._result()
    
    
    def __parsePage(self, cate='', event=0, num=0, retry=3, pause=0.001):
        for _ in range(retry):
            time.sleep(pause)
            
            try:
                rdInt = Utility.random()
                request = self._session.get( cf.MACRO_URL % (rdInt, cate, event, num, rdInt), timeout=10 )
                if self._PY3:
                    request.encoding = 'gbk'
                    
                regSym = re.compile(r'\,count:(.*?)\}')
                datastr = regSym.findall(request.text)
                datastr = datastr[0]
                datastr = datastr.split('data:')[1]
            except Exception as e:
                print(e)
            else:
                return datastr
            
        raise IOError(cf.NETWORK_URL_ERROR_MSG)
    
    
    def shibor(self, year=None):
        """
        获取上海银行间同业拆放利率
        Parameters
        ------
          year:年份(int)
          
        Return
        ------
        DataFrame or List: [{'date':, 'ON':, ...}, ...]
            date:日期
            ON:隔夜拆放利率
            1W:1周拆放利率
            2W:2周拆放利率
            1M:1个月拆放利率
            3M:3个月拆放利率
            6M:6个月拆放利率
            9M:9个月拆放利率
            1Y:1年拆放利率
        """
        self._data = pd.DataFrame()
        
        lab = cf.SHIBOR_TYPE['Shibor']
        # http://www.shibor.org/shibor/web/html/downLoad.html?nameNew=Historical_Shibor_Data_2018.xls&downLoadPath=data&nameOld=Shibor数据2018.xls&shiborSrc=http://www.shibor.org/shibor/
        self._data = self.__parseExcel(year, 'Shibor', lab, cf.SHIBOR_COLS)
        
        return self._result()
        
    
    def shiborQuote(self, year=None):
        """
        获取Shibor银行报价数据
        Parameters
        ------
          year:年份(int)
          
        Return
        ------
        DataFrame or List: [{'date':, 'bank':, ...}, ...]
            date:日期
            bank:报价银行名称
            ON:隔夜拆放利率
            1W:1周拆放利率
            2W:2周拆放利率
            1M:1个月拆放利率
            3M:3个月拆放利率
            6M:6个月拆放利率
            9M:9个月拆放利率
            1Y:1年拆放利率
        """
        self._data = pd.DataFrame()
        
        lab = cf.SHIBOR_TYPE['Quote']
        # http://www.shibor.org/shibor/web/html/downLoad.html?nameNew=Historical_Quote_Data_2018.xls&downLoadPath=data&nameOld=报价数据2018.xls&shiborSrc=http://www.shibor.org/shibor/
        self._data = self.__parseExcel(year, 'Quote', lab, cf.QUOTE_COLS)
        
        return self._result()
    
    
    def shiborMa(self, year=None):
        """
        获取Shibor均值数据
        Parameters
        ------
          year:年份(int)
          
        Return
        ------
        DataFrame or List: [{'date':, 'ON_5':, ...}, ...]
            date:日期
               其它分别为各周期5、10、20均价
        """
        self._data = pd.DataFrame()
        
        lab = cf.SHIBOR_TYPE['Tendency']
        self._data = self.__parseExcel(year, 'Shibor_Tendency', lab, cf.SHIBOR_MA_COLS)
        
        return self._result()
    
    
    def lpr(self, year=None):
        """
        获取贷款基础利率
        Parameters
        ------
          year:年份(int)
          
        Return
        ------
        DataFrame or List: [{'date':, '1Y':, ...}, ...]
            date:日期
            1Y:1年贷款基础利率
        """
        self._data = pd.DataFrame()
        
        lab = cf.SHIBOR_TYPE['LPR']
        self._data = self.__parseExcel(year, 'LPR', lab, cf.LPR_COLS)
        
        return self._result()
    
    
    def lprMa(self, year=None):
        """
        获取贷款基础利率均值数据
        Parameters
        ------
          year:年份(int)
          
        Return
        ------
        DataFrame or List: [{'date':, '1Y_5':, ...}, ...]
            date:日期
            1Y_5:5日均值
            1Y_10:10日均值
            1Y_20:20日均值
        """
        self._data = pd.DataFrame()
        
        lab = cf.SHIBOR_TYPE['LPR_Tendency']
        self._data = self.__parseExcel(year, 'LPR_Tendency', lab, cf.LPR_MA_COLS)
        
        return self._result()
        
        
    def __parseExcel(self, year, datatype, lab, column):
        year = Utility.getYear() if year is None else year
        lab = lab.encode('utf-8') if self._PY3 else lab
        
        try:
            df = pd.read_excel( cf.SHIBOR_DATA_URL % (datatype, year, lab, year), skiprows=[0] )
            df.columns = column
            df['date'] = df['date'].map(lambda x: x.date())
            df['date'] = df['date'].astype('datetime64[ns]')
        except Exception as e:
            print(e)
        else:
            return df
        
        
