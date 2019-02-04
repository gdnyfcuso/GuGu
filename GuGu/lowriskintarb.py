# -*- coding:utf-8 -*-
"""
低风险及套利类
Created on 2019/01/30
@author: TabQ
@group : GuGu
@contact: 16621596@qq.com
"""

import re
import json
import pandas as pd
import numpy as np
from base import Base, cf

class LowRiskIntArb(Base):
    def ratingFundA(self):
        """
        分级基金A及其相关数据
        return
        ------
        DataFrame or List: [{'funda_id':, 'funda_name':, ...}, ...]
            funda_id:                    分级A代码
            funda_name:                  分级A名称
            funda_current_price:         现价
            funda_increase_rt:           涨幅（％）
            funda_volume:                成交额
            funda_value:                 净值
            funda_discount_rt:           折价率（％）
            funda_coupon:                本期利率
            funda_coupon_next:           下期利率
            funda_profit_rt_next:        修正收益率（％）
            funda_index_id:              参考指数代码
            funda_index_name:            参考指数名称
            funda_index_increase_rt:     指数涨幅（％）
            funda_lower_recalc_rt:       下折母基需跌（％）
            lower_recalc_profit_rt:      理论下折收益（％）
            fundb_upper_recalc_rt:       上折母基需涨（％）
            funda_base_est_dis_rt_t1:    T-1溢价率（％）
            funda_base_est_dis_rt_t2:    T-2溢价率（％）
            funda_amount:                A份额（万份）
            funda_amount_increase:       A新增（万份）
            abrate:                      A:B
            next_recalc_dt:              下次定折日期
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.RATING_FUNDA_URL, cf.RATING_FUNDA_COLS)
        self._data[self._data=="-"] = np.NaN
        self._data['next_recalc_dt'] = self._data['next_recalc_dt'].map(self.__nextRecalcDt)
        for col in ['funda_current_price', 'funda_increase_rt', 'funda_volume', 'funda_value',
                    'funda_discount_rt', 'funda_coupon', 'funda_coupon_next', 'funda_profit_rt_next', 
                    'funda_index_increase_rt', 'funda_lower_recalc_rt', 'lower_recalc_profit_rt', 
                    'fundb_upper_recalc_rt', 'funda_base_est_dis_rt_t1', 'funda_base_est_dis_rt_t2', 
                    'funda_amount_increase']:
            self._data[col] = self._data[col].astype(float)
        
        return self._result()
    
    
    def __nextRecalcDt(self, x):
        pattern = re.compile(r'<span.*?>(\d{4}-\d{2}-\d{2})</span>')
        
        return re.sub(pattern, r'\1', x)
    
    
    def ratingFundB(self):
        """
        分级基金A及其相关数据
        return
        ------
        DataFrame or List: [{'fundb_id':, 'fundb_name':, ...}, ...]
            fundb_id:                            分级B代码
            fundb_name:                          分级B名称
            fundb_base_fund_id:                  对应母基代码
            funda_id:                            分级A代码
            funda_name:                          分级A名称
            coupon:                              优惠
            manage_fee:                          管理费
            funda_current_price:                 分级A现价
            funda_upper_price:                   分级A最高价
            funda_lower_price:                   分级A最低价
            funda_increase_rt：                  分级A增长率（％）
            fundb_current_price：                分级B现价
            fundb_upper_price：                  分级B最高价
            fundb_lower_price：                  分级B最低价
            fundb_increase_rt：                  分级B增长率（％）
            fundb_volume：                       分级B成交额（万元）
            fundb_value：                        分级B净值
            fundm_value：                        母基净值
            fundb_discount_rt：                  分级B溢价率（％）
            fundb_price_leverage_rt：            分级B价格杠杆
            fundb_net_leverage_rt：              分级B净值杠杆
            fundb_capital_rasising_rt：          分级B融资成本（％）
            fundb_lower_recalc_rt：              下折母基需跌（％）
            fundb_upper_recalc_rt：              上折母基需涨（％）
            b_est_val：                          估值
            fundb_index_id：                     参考指数代码
            fundb_index_name：                   参考指数名称
            fundb_index_increase_rt：            参考指数涨幅（％）
            funda_ratio：                        分级A占比
            fundb_ratio：                        分级B占比
            fundb_base_price：                   分级B基础价格
            fundB_amount：                       B份额（万份）
            fundB_amount_increase：              B新增份额（万份）
            abrate：                             A：B
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.RATING_FUNDB_URL, cf.RATING_FUNDB_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['coupon', 'manage_fee', 'funda_current_price', 'funda_upper_price', 'funda_lower_price',
                    'funda_increase_rt', 'fundb_current_price', 'fundb_upper_price', 'fundb_lower_price',
                    'fundb_increase_rt', 'fundb_volume', 'fundb_value', 'fundm_value', 'fundb_discount_rt',
                    'fundb_price_leverage_rt', 'fundb_net_leverage_rt', 'fundb_capital_rasising_rt', 
                    'fundb_lower_recalc_rt', 'fundb_upper_recalc_rt', 'b_est_val', 'fundb_index_increase_rt', 
                    'fundb_base_price', 'fundB_amount', 'fundB_amount_increase']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def ratingFundM(self):
        """
        分级基金A及其相关数据
        return
        ------
        DataFrame or List: [{}]
            base_fund_id:                    母基代码
            base_fund_nm：                   母基名称
            market：                         所属市场
            issue_dt：                       创立日期
            manage_fee：                     管理费
            index_id：                       跟踪指数代码
            index_nm：                       跟踪指数名称
            lower_recalc_price：             下折
            a_ratio：                        A占比
            b_ratio：                        B占比
            next_recalc_dt：                 下次定折
            fundA_id：                       A基代码
            fundA_nm：                       A基名称
            coupon：                         本期利率
            coupon_next：                    下期利率
            fundB_id：                       B基代码
            fundB_nm:                        B基名称
            price：                          母基净值
            base_lower_recalc_rt：           下折需跌（％）
            abrate：                         A：B
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.RATING_FUNDM_URL, cf.RATING_FUNDM_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['manage_fee', 'lower_recalc_price', 'a_ratio', 'b_ratio', 'coupon', 'coupon_next',
                    'price', 'base_lower_recalc_rt']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def conBonds(self):
        """
        可转债及其相关数据
        return
        ------
        DataFrame or List: [{'bond_id':, 'bond_nm':, ...}, ...]
            bond_id:                        代码
            bond_nm:                        名称
            stock_id:                       正股全代码
            stock_nm:                       正股名称
            market:                         所属市场
            convert_price:                  转股价
            convert_dt:                     转股起始日
            issue_dt：                      创立日期
            maturity_dt：                   到期日
            next_put_dt：                   回售起始日
            put_price：                     回售价
            put_count_days：                回售计算天数
            put_total_days：                回售总天数
            redeem_price：                  赎回价
            redeem_price_ratio：            赎回率
            redeem_count_days：             赎回计算天数
            redeem_total_days：             赎回总天数
            orig_iss_amt：                  发行规模（亿）
            curr_iss_amt：                  剩余规模（亿）
            rating_cd：                     债券评级
            issuer_rating_cd：              主体评级
            guarantor：                     担保
            active_fl：                     活跃标志
            ration_rt：                     股东配售率（％）
            pb：                            市净率
            sprice：                        正股价
            sincrease_rt：                  正股涨跌（％）
            last_time：                     最后时间
            convert_value：                 转股价值
            premium_rt：                    溢价率（％）
            year_left：                     剩余年限
            ytm_rt：                        到期税前收益（％）
            ytm_rt_tax：                    到期税后收益（％）
            price：                         现价
            increase_rt：                   涨跌幅（％）
            volume：                        成交额（万元）
            force_redeem_price：            强赎触发价
            put_convert_price：             回售触发价
            convert_amt_ratio：             转债占比
            stock_cd：                      正股简码
            pre_bond_id：                   转债全码
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.CON_BONDS_URL, cf.CON_BONDS_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['convert_price', 'put_price', 'redeem_price', 'redeem_price_ratio', 'orig_iss_amt',
                    'curr_iss_amt', 'ration_rt', 'pb', 'sprice', 'sincrease_rt', 'convert_value', 'premium_rt',
                    'year_left', 'ytm_rt', 'ytm_rt_tax', 'price', 'increase_rt', 'volume', 'force_redeem_price',
                    'put_convert_price', 'convert_amt_ratio']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def closedStockFund(self):
        """
        封闭股基及其相关数据
        return
        ------
        DataFrame or List: [{'fund_id':, 'fund_nm':, ...}, ...]
            fund_id:                    代码
            fund_nm:                    名称
            issue_dt:                   创立日期
            duration:                   持续时间
            last_time:                  最后时间
            price:                      现价
            increase_rt：               涨幅（％）
            volume：                    成交金额（万）
            net_value：                 净值
            nav_dt：                    净值日期
            realtime_estimate_value：   最近估值
            discount_rt：               折价率（％）
            left_year：                 剩余年限
            annualize_dscnt_rt：        年化折价率（％）
            quote_incr_rt：             周价增（％）
            nav_incr_rt：               周净增（％）
            spread：                    净价差（％）
            stock_ratio：               股票占比（％）
            report_dt：                 报告日期
            daily_nav_incr_rt：         当日净增（％）
            daily_spread：              日净价差（％）
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.CLOSED_STOCK_FUND_URL, cf.CLOSED_STOCK_FUND_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['price', 'increase_rt', 'volume', 'net_value', 'realtime_estimate_value', 'discount_rt',
                    'left_year', 'annualize_dscnt_rt', 'quote_incr_rt', 'nav_incr_rt', 'spread', 'stock_ratio',
                    'daily_nav_incr_rt', 'daily_spread']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def closedBondFund(self):
        """
        封闭债基及其相关数据
        return
        ------
        DataFrame or List: [{'fund_id':, 'fund_nm':, ...}, ...]
            fund_id:                    代码
            fund_nm:                    名称
            maturity_dt:                到期日期
            left_year:                  剩余年限
            est_val：                   最近估值
            discount_rt：               折价率（％）
            annual_discount_rt：        年化折价率（％）
            trade_price：               现价
            increase_rt：               涨幅（％）
            volume：                    成交金额
            last_time:                  最后时间
            fund_nav：                  最近净值
            last_chg_dt：               净值日期
            price_incr_rt：             净值日增（％）
            stock_ratio：               股票比例
            bond_ratio：                债券比例
            report_dt：                 报告日期
            is_outdate：                是否超期
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.CLOSED_BOND_FUND_URL, cf.CLOSED_BOND_FUND_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['left_year', 'est_val', 'discount_rt', 'annual_discount_rt', 'trade_price',
                    'increase_rt', 'volume', 'fund_nav', 'price_incr_rt', 'stock_ratio', 'bond_ratio']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def AHRatio(self):
        """
        A/H比价
        return
        ------
        DataFrame or List: [{'a_code':, 'stock_name':, ...}, ...]
            a_code:                A股代码
            stock_name:            股票名称
            a_price:               A股价格
            a_increase_rt：        A股涨跌幅（％）
            h_code:                H股代码
            h_price:               H股价格（港元）
            h_increase_rt：        H股涨跌幅（％）
            last_time:            最后时间
            rmb_price：            H股价格（人民币）
            hk_currency：          港元汇率
            ha_ratio：             比价（H/A）
            h_free_shares:        H股自由流通市值（亿港元）
            a_free_shares：        A股自由流通市值（亿元）
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.AH_RATIO_URL, cf.AH_RATIO_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['a_price', 'a_increase_rt', 'h_price', 'h_increase_rt', 'rmb_price',
                    'hk_currency', 'ha_ratio', 'h_free_shares', 'a_free_shares']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def dividendRate(self):
        """
        A股股息率
        return
        ------
        DataFrame or List: [{'stock_id':, 'stock_nm':, ...}, ...]
            stock_id:                            股票代码
            stock_nm:                            股票名称
            dividend_rate:                       股息率（TTM）
            dividend_rate2：                     静态股息率
            ipo_date：                           ipo日期
            price：                              价格
            volume：                             成交额（万元）
            increase_rt：                        涨幅（％）
            pe:                                  市盈率（TTM）
            pb:                                  市净率
            total_value：                        市值（亿元）
            eps_growth_ttm：                     净利同比增长（％）
            roe:                                 最新年报ROE（％）
            revenue_average：                    5年营收复合增长（％）
            profit_average：                     5年利润复合增长（％）
            roe_average：                        5年平均ROE（％）
            pb_temperature：                     PB温度（℃）
            pe_temperature：                     PE温度（℃）
            int_debt_rate：                      有息负债率（％）
            cashflow_average：                    5年现金流复合增长（％）
            dividend_rate_average：               5年分红率复合增长（％）
            dividend_rate5：                      5年平均股息率（％）
            industry_nm：                         所属行业
            active_flg:                          活跃标志
            last_time:                           最后时间
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.DIVIDEND_RATE_URL, cf.DIVIDEND_RATE_COLS)
        self._data[self._data=="-"] = np.NaN
        for col in ['dividend_rate', 'dividend_rate2', 'price', 'volume', 'increase_rt', 'pe', 'pb', 'total_value',
                    'eps_growth_ttm', 'roe', 'revenue_average', 'profit_average', 'roe_average', 'pb_temperature',
                    'pe_temperature', 'int_debt_rate', 'cashflow_average', 'dividend_rate_average', 'dividend_rate5']:
            self._data[col] = self._data[col].astype(float)
            
        return self._result()
    
    
    def stockLof(self):
        """
        股票LOF基金及基相关数据
        return
        ------
        DataFrame or List: [{'fund_id':, 'fund_nm':, ...}, ...]
            fund_id:                基金代码
            fund_nm:                基金名称
            price:                  现价  
            increase_rt:            涨幅（％）
            volume:                 成交（万元）
            amount:                 场内份额（万份）
            fund_nav:               基金净值
            nav_dt:                 净值日期
            estimate_value:         实时估值
            discount_rt:            溢价率（％）
            stock_ratio:            股票占比（％）
            stock_increase_rt:      重仓涨幅（％）
            apply_fee:              申购费（％）
            redeem_fee:             赎回费（％）
            apply_redeem_status:    申赎状态
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.STOCK_LOF_URL, cf.STOCK_LOF_COLS)
        for col in ['price', 'increase_rt', 'volume', 'amount', 'fund_nav', 
                    'estimate_value', 'discount_rt', 'stock_ratio', 'stock_increase_rt', 'apply_fee', 'redeem_fee']:
            self._data[col] = self._data[col].astype(float)
        
        return self._result()
    
    
    def indexLof(self):
        """
        指数LOF基金及基相关数据
        return
        ------
        DataFrame or List: [{'fund_id':, 'fund_nm':, ...}, ...]
            fund_id:                基金代码
            fund_nm:                基金名称
            price:                  现价  
            increase_rt:            涨幅（％）
            volume:                 成交（万元）
            amount:                 场内份额（万份）
            fund_nav:               基金净值
            nav_dt:                 净值日期
            estimate_value:         实时估值
            discount_rt:            溢价率（％）
            index_id:               指数代码
            index_nm:               指数名称
            index_increase_rt:      指数涨幅（％）
            apply_fee:              申购费（％）
            redeem_fee:             赎回费（％）
            apply_redeem_status:    申赎状态
        """
        self._data = pd.DataFrame()
        
        self._data = self.__parsePage(cf.INDEX_LOF_URL, cf.INDEX_LOF_COLS)
        for col in ['price', 'increase_rt', 'volume', 'amount', 'fund_nav', 'estimate_value', 
                    'discount_rt', 'index_increase_rt', 'apply_fee', 'redeem_fee']:
            self._data[col] = self._data[col].astype(float)
        
        return self._result()
    
    
    def __parsePage(self, url, column):            
        page = 1
        while(True):
            try:
                request = self._session.get(url % page)
                text = request.text.replace('%', '')
                dataDict = json.loads(text)
                if dataDict['page'] < page:
                    break
                
                dataList = []
                for row in dataDict['rows']:
                    dataList.append(row['cell'])
                  
                self._data = self._data.append(pd.DataFrame(dataList, columns = column), ignore_index=True)
                
                page += 1
            except Exception as e:
                print(str(e))
            
        return self._data
    
    
