import sys, os, time, threading, datetime
sys.path.append( '../utils' )
from utils import Utils, SAVE_DATA, LOG, ERROR
import tushare as ts
import pandas as pd
import numpy as np
#wmcloud pw qingxue@1990
from multiprocessing import Queue, Process

'''--------------- Data class ---------------'''
class Data():

	def __init__( self, date = None ):
	
		self.__date = date
		if SAVE_DATA == 'xls':
			if self.__date is None:
			# 如果没有date输入，找到最新的数据文件
				file_date = datetime.date.today()	
				self.__date = file_date.strftime( '%Y_%m_%d' )
				self.__main_dir = '../data/data_' + self.__date

				while not os.path.exists( self.__main_dir ):
					file_date -= datetime.timedelta( days = 1 )
					self.__date = file_date.strftime( '%Y_%m_%d' )
					self.__main_dir = '../data/data_' + self.__date
			else:
				self.__main_dir = '../data/data_' + self.__date

			self.__basics_file_name = self.__main_dir + '/stock_basics.xlsx'
			self.__quarter_report_file_name = self.__main_dir + '/stock_quarter_report.xlsx'
			self.__profit_file_name = self.__main_dir + '/stock_profit.xlsx'
			self.__operation_file_name = self.__main_dir + '/stock_operation.xlsx'
			self.__growth_file_name = self.__main_dir + '/stock_growth.xlsx'
			self.__debtpaying_file_name = self.__main_dir + '/stock_debtpaying.xlsx'
			self.__cashflow_file_name = self.__main_dir + '/stock_cashflow.xlsx'
			self.__k_line_file_path = self.__main_dir + '/stock_k_line/'
			self.__divi_file_name = self.__main_dir + '/stock_divi.xlsx'
			self.__forcast_quarter_report_file_name = self.__main_dir + '/stock_forcast_quarter_report.xlsx'
			self.__restrict_stock_file_name = self.__main_dir + '/stock_restrict_stock.xlsx'
			self.__concept_classified_file_name = self.__main_dir + '/stock_concept_classified.xlsx'
	
	'''--------------- stock fundamental data ---------------'''
	def get_realtime_quotes( self, code ):
	# 返回值：
	# 0：name，股票名字
	# 1：open，今日开盘价
	# 2：pre_close，昨日收盘价
	# 3：price，当前价格
	# 4：high，今日最高价
	# 5：low，今日最低价
	# 6：bid，竞买价，即“买一”报价
	# 7：ask，竞卖价，即“卖一”报价
	# 8：volume，成交量 maybe you need do volume/100
	# 9：amount，成交金额（元 CNY）
	# 10：b1_v，委买一（笔数 bid volume）
	# 11：b1_p，委买一（价格 bid price）
	# 12：b2_v，“买二”
	# 13：b2_p，“买二”
	# 14：b3_v，“买三”
	# 15：b3_p，“买三”
	# 16：b4_v，“买四”
	# 17：b4_p，“买四”
	# 18：b5_v，“买五”
	# 19：b5_p，“买五”
	# 20：a1_v，委卖一（笔数 ask volume）
	# 21：a1_p，委卖一（价格 ask price）
	# ...
	# 30：date，日期；
	# 31：time，时间；
		return ts.get_realtime_quotes( code )
		
	def update_stock_basics( self ):
		
		if not os.path.exists( self.__basics_file_name ):			
			try:
				df_stock_basics = ts.get_stock_basics()	
			except:
				ERROR( 'exception occurs when update stock_basics' )
			else:
				LOG( 'update basics data' )
				Utils.save_data( df_stock_basics, self.__basics_file_name, 'stock basics' )
	
	def get_stock_basics( self ):
	# 返回值：
	# code,代码
	# name,名称
	# industry,所属行业
	# area,地区
	# pe,市盈率
	# outstanding,流通股本(亿)
	# totals,总股本(亿)
	# totalAssets,总资产(万) = 净资产 + 负债 = （ 股东权益 + 少数股东权益 ） + 负债
	# liquidAssets,流动资产
	# fixedAssets,固定资产
	# reserved,公积金
	# reservedPerShare,每股公积金
	# esp,每股收益
	# bvps,每股净资产 = （ 股东权益 + 少数股东权益 ） / 总股本
	# pb,市净率 = 股价 / 每股净资产
	# timeToMarket,上市日期
	# undp,未分利润
	# perundp, 每股未分配
	# rev,收入同比(%)
	# profit,利润同比(%)
	# gpr,毛利率(%)
	# npr,净利润率(%)
	# holders,股东人数	
		if os.path.exists( self.__basics_file_name ):
			return Utils.read_data( self.__basics_file_name )
		else:
			ERROR( self.__basics_file_name + ' not exists' )
			exit()
	
	def update_quarter_report_data( self ):
		
		if not os.path.exists( self.__quarter_report_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_quarter_report_data = pd.DataFrame()
			for year in range( 2016, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_report_data( year, quarter )
					except:
						ERROR( 'exception occurs when update quarter report data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_quarter_report_data = df_quarter_report_data.append( df_tmp )
						LOG( 'update quarter report data of year {0} quarter {1}'.format( year, quarter ) )
						
			Utils.save_data( df_quarter_report_data, self.__quarter_report_file_name, 'quarter report' )
	
	def get_quarter_report_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# esp,每股收益
	# eps_yoy,每股收益同比(%)
	# bvps,每股净资产
	# roe,净资产收益率(%)
	# epcf,每股现金流量(元)
	# net_profits,净利润(万元)
	# profits_yoy,净利润同比(%)
	# distrib,分配方案
	# report_date,发布日期
		if os.path.exists( self.__quarter_report_file_name ):
			return Utils.read_data( self.__quarter_report_file_name )
		else:
			ERROR( self.__quarter_report_file_name + ' not exists' )
			exit()
			
	def update_profit_data( self ):
		
		if not os.path.exists( self.__profit_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_profit_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_profit_data( year, quarter )
					except:
						ERROR( 'exception occurs when update profit data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_profit_data = df_profit_data.append( df_tmp )
						LOG( 'update profit data of year {0} quarter {1}'.format( year, quarter ) )
			# 解决 ts.get_growth_data() 拿不到数据时一直等待的问题
			#self.update_growth_data()
			Utils.save_data( df_profit_data, self.__profit_file_name, 'quarter report' )
	
	def get_profit_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# roe,净资产收益率(%)
	# net_profit_ratio,净利率(%)
	# gross_profit_rate,毛利率(%)
	# net_profits,净利润(万元)
	# eps,每股收益
	# business_income,营业收入(百万元)
	# bips,每股主营业务收入(元)
		if os.path.exists( self.__profit_file_name ):
			return Utils.read_data( self.__profit_file_name )
		else:
			ERROR( self.__profit_file_name + ' not exists' )
			exit()
	
	def update_operation_data( self ):
		
		if not os.path.exists( self.__operation_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_operation_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_operation_data( year, quarter )
					except:
						ERROR( 'exception occurs when update operation data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_operation_data = df_operation_data.append( df_tmp )
						LOG( 'update operation data of year {0} quarter {1}'.format( year, quarter ) )
			Utils.save_data( df_operation_data, self.__operation_file_name, 'operation' )
	
	def get_operation_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# arturnover,应收账款周转率(次)
	# arturndays,应收账款周转天数(天)
	# inventory_turnover,存货周转率(次)
	# inventory_days,存货周转天数(天)
	# currentasset_turnover,流动资产周转率(次)
	# currentasset_days,流动资产周转天数(天)
		if os.path.exists( self.__operation_file_name ):
			return Utils.read_data( self.__operation_file_name )
		else:
			ERROR( self.__operation_file_name + ' not exists' )
			exit()
			
	def update_growth_data( self ):
		
		if not os.path.exists( self.__growth_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_growth_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_growth_data( year, quarter )
					except:
						ERROR( 'exception occurs when update growth data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_growth_data = df_growth_data.append( df_tmp )
						LOG( 'update growth data of year {0} quarter {1}'.format( year, quarter ) )
			Utils.save_data( df_growth_data, self.__growth_file_name, 'growth' )
	
	def get_growth_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# mbrg,主营业务收入增长率(%)
	# nprg,净利润增长率(%)
	# nav,净资产增长率
	# targ,总资产增长率
	# epsg,每股收益增长率
	# seg,股东权益增长率
		if os.path.exists( self.__growth_file_name ):
			return Utils.read_data( self.__growth_file_name )
		else:
			ERROR( self.__growth_file_name + ' not exists' )
			exit()
	
	def update_debtpaying_data( self ):
		
		if not os.path.exists( self.__debtpaying_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_debtpaying_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_debtpaying_data( year, quarter )
					except:
						ERROR( 'exception occurs when update debtpaying data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_debtpaying_data = df_debtpaying_data.append( df_tmp )
						LOG( 'update debtpaying data of year {0} quarter {1}'.format( year, quarter ) )
			Utils.save_data( df_debtpaying_data, self.__debtpaying_file_name, 'debtpaying' )
	
	def get_debtpaying_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# currentratio,流动比率 = 流动资产 / 流动负债 ( 一般高于2 )
	# quickratio,速动比率 = 速动资产 / 流动负债
	# cashratio,现金比率
	# icratio,利息支付倍数 = 税息前利润 / 利息费用 ( 一般高于1.5 )
	# sheqratio,股东权益比率 = 股东权益总额 / 资产总额 ( 高于0.6 )
	# adratio,股东权益增长率
		if os.path.exists( self.__debtpaying_file_name ):
			return Utils.read_data( self.__debtpaying_file_name )
		else:
			ERROR( self.__debtpaying_file_name + ' not exists' )
			exit()	
	
	def update_cashflow_data( self ):
		
		if not os.path.exists( self.__cashflow_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_cashflow_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
					try:
						df_tmp = ts.get_cashflow_data( year, quarter )
					except:
						ERROR( 'exception occurs when update cashflow data of year {0} quarter {1}'.format( year, quarter ) )
						return
					else:
						df_tmp[ 'year' ] = year
						df_tmp[ 'quarter' ] = quarter
						df_cashflow_data = df_cashflow_data.append( df_tmp )
						LOG( 'update cashflow data of year {0} quarter {1}'.format( year, quarter ) )	
			Utils.save_data( df_cashflow_data, self.__cashflow_file_name, 'cashflow' )
	
	def get_cashflow_data( self ):
	# code,代码
	# name,名称
	# cf_sales,经营现金净流量对销售收入比率
	# rateofreturn,资产的经营现金流量回报率
	# cf_nm,经营现金净流量与净利润的比率
	# cf_liabilities,经营现金净流量对负债比率
	# cashflowratio,现金流量比率
		if os.path.exists( self.__cashflow_file_name ):
			return Utils.read_data( self.__cashflow_file_name )
		else:
			ERROR( self.__cashflow_file_name + ' not exists' )
			exit()	
			
			
	'''--------------- stock transaction data ---------------'''			
	def update_k_line_data( self, num_threads = 8 ):
		
		def update_k_line_data_range( self, df_stock_basics, id_start, id_end ):
		
			for i in range( id_start, id_end ):		
				code = '%06d' % int( df_stock_basics['code'][i] )
				cur_k_line_file = self.__k_line_file_path + code + '.xlsx'
				if not os.path.exists( cur_k_line_file ):	
					try:
						df_k_line_data = ts.get_k_data( code, ktype = 'D', autype = 'qfq' )
					except:
						ERROR( 'exception occurs when update k_line data {0}'.format( code ) )
					else:
						LOG( '{3} update {0} k_line data {1:4d}/{2:4d} '.format( code, i + 1, num_stock, threading.current_thread().getName() ) )
						Utils.save_data( df_k_line_data, cur_k_line_file, code )
		
		if not os.path.exists( self.__k_line_file_path ):
			os.mkdir( self.__k_line_file_path )
		
		df_tmp_stock_basics = Utils.read_data( self.__basics_file_name )
		num_stock = df_tmp_stock_basics.index.size
		
		list_threads = []
		for n in range( num_threads ):
			list_threads.append( threading.Thread( target = update_k_line_data_range, \
				args=( self, df_tmp_stock_basics, int( num_stock / num_threads ) * n, int( num_stock / num_threads ) * ( n + 1 ) ) ) )
		list_threads.append( threading.Thread( target = update_k_line_data_range, \
			args=( self, df_tmp_stock_basics, int( num_stock / num_threads ) * num_threads, num_stock ) ) )
		
		for thread in list_threads:
			thread.start()
		for thread in list_threads:
			thread.join()
		
		# 解决多线程可能丢失数据问题
		update_k_line_data_range( self, df_tmp_stock_basics, 0, num_stock )
	
	def get_k_line_data( self, code ):

		cur_stock_k_line_file = self.__k_line_file_path + code + '.xlsx'	
		
		if os.path.exists( cur_stock_k_line_file ):
			return Utils.read_data( cur_stock_k_line_file )
		else:
			ERROR( cur_stock_k_line_file + ' not exists' )
			exit()
			
	def get_k_line_date_by_reverse_days( self, code, reverse_days ):
		k_data = self.get_k_line_data( code )
		if k_data.index.size - reverse_days >= 0 and reverse_days >= 0:
			return k_data.get( 'date' )[ k_data.index.size - reverse_days ]
		else:
			return ''
	
	def get_k_line_date_by_sequential_days( self, code, sequential_days ):
		k_data = self.get_k_line_data( code )
		num_market_days = k_data.index.size
		if sequential_days >= 0 and sequential_days < num_market_days:
			return k_data.get( 'date' )[ sequential_days ]
		else:
			return ''
	
	'''--------------- reference data ---------------'''
	def update_divi_data( self ):
		
		def download_divi_data( self, year, top ):
			try:
				df_tmp = ts.profit_data( year, top )
			except:
				ERROR( 'exception occurs when update divi data of year {0}'.format( year ) )
				return pd.DataFrame()
			else:
				LOG( 'update divi data of year {0}'.format( year ) )
				thread_queue.put( df_tmp )
		if not os.path.exists( self.__divi_file_name ):
			cur_year = time.strftime( '%Y',time.localtime( time.time() ) )
			df_tmp_stock_basics = Utils.read_data( self.__basics_file_name )			
			num_stock = df_tmp_stock_basics.index.size
			df_divi_data = pd.DataFrame()
			thread_queue = Queue()
			list_threads = []
			for year in range( 2002, int( Utils.cur_date().split('_')[ 0 ] ) + 1 ):
				list_threads.append( threading.Thread( target = download_divi_data, \
					args=( self, year, num_stock ) ) )
			
			for thread in list_threads:
				thread.start()
			for thread in list_threads:
				thread.join()
			while not thread_queue.empty():
				df_divi_data = df_divi_data.append( thread_queue.get() )
			Utils.save_data( df_divi_data, self.__divi_file_name, 'divi data' )
	
	def get_divi_data( self ):
	# 返回值：
	# code:股票代码
	# name:股票名称
	# year:分配年份
	# report_date:公布日期
	# divi:分红金额（每10股）
	# shares:转增和送股数（每10股）
		if os.path.exists( self.__divi_file_name ):
			return Utils.read_data( self.__divi_file_name )
		else:
			ERROR( self.__divi_file_name + ' not exists' )
			exit()
		
	def update_forcast_quarter_report_data( self ):
		
		if not os.path.exists( self.__forcast_quarter_report_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_forcast_quarter_report_data = pd.DataFrame()
			for year in range( 2003, int( list_date_quarter[0] ) + 1 ):
				for quarter in range( 1, 5 ):
					try:
						df_tmp = ts.forecast_data( year, quarter )
					except:
						ERROR( 'exception occurs when update forcast quarter report data of year {0} quarter {1}'.format( year, quarter ) )
					else:
						df_forcast_quarter_report_data = df_forcast_quarter_report_data.append( df_tmp )
						LOG( 'update forcast quarter report data of year {0} quarter {1}'.format( year, quarter ) )
					if year == int( list_date_quarter[0] ) and quarter == list_date_quarter[3]:		
						break
			Utils.save_data( df_forcast_quarter_report_data, self.__forcast_quarter_report_file_name, 'forcast quarter report' )
	
	def get_forcast_quarter_report_data( self ):
	# 返回值：
	# code,代码
	# name,名称
	# type,业绩变动类型【预增、预亏等】
	# report_date,发布日期
	# pre_eps,上年同期每股收益
	# range,业绩变动范围
		if os.path.exists( self.__forcast_quarter_report_file_name ):
			return Utils.read_data( self.__forcast_quarter_report_file_name )
		else:
			ERROR( self.__forcast_quarter_report_file_name + ' not exists' )
			exit()
	
	def update_restrict_stock_data( self, extend_years = 3 ):
		
		if not os.path.exists( self.__restrict_stock_file_name ):
			list_date_quarter = Utils.parse_date_to_ymd( self.__date )
			df_restrict_stock_data = pd.DataFrame()
			
			for year in range( 2010, int( list_date_quarter[0] ) + extend_years + 1 ):
				for month in range( 1, 13 ):
					try:
						df_tmp = ts.xsg_data( year, month )
					except:
						ERROR( 'exception occurs when update restrict stock data of year {0} month {1}'.format( year, month ) )
					else:
						df_restrict_stock_data = df_restrict_stock_data.append( df_tmp )
						LOG( 'update restrict stock data of year {0} month {1}'.format( year, month ) )
					if year == int( list_date_quarter[0] ) + extend_years  and month == int( list_date_quarter[1] ):		
						break
			Utils.save_data( df_restrict_stock_data, self.__restrict_stock_file_name, 'restrict stock' )
			
	def get_restrict_stock_data( self ):
	# 返回值：
	# code：股票代码
	# name：股票名称
	# date:解禁日期
	# count:解禁数量（万股）
	# ratio:占总盘比率
		if os.path.exists( self.__restrict_stock_file_name ):
			return Utils.read_data( self.__restrict_stock_file_name )
		else:
			ERROR( self.__restrict_stock_file_name + ' not exists' )
			exit()
	
	'''--------------- stock classification data ---------------'''	
	def update_concept_classified( self ):
		
		if not os.path.exists( self.__concept_classified_file_name ):			
			try:
				df_concept_classified = ts.get_concept_classified()
			except:
				ERROR( 'exception occurs when update concept classified data' )
			else:
				LOG( 'update concept classified' )
				Utils.save_data( df_concept_classified, self.__concept_classified_file_name, 'concept classified' )	
	
	def get_concept_classified_data( self ):
	# 返回值：
	# code：股票代码
	# name：股票名称
	# c_name：概念名称
		if os.path.exists( self.__concept_classified_file_name ):
			return Utils.read_data( self.__concept_classified_file_name )
		else:
			ERROR( self.__concept_classified_file_name + ' not exists' )
			exit()
	
	'''--------------- update all latest data ---------------'''	
	@Utils.func_timer
	def update_all( self ):
		
		if not os.path.exists( self.__main_dir ):
			os.mkdir( self.__main_dir )
		self.update_stock_basics()
	
		list_process = []		
		list_process.append( Process( target = self.update_quarter_report_data ) )
		list_process.append( Process( target = self.update_profit_data ) ) # include update_growth_data
		list_process.append( Process( target = self.update_growth_data ) )
		list_process.append( Process( target = self.update_operation_data ) )
		list_process.append( Process( target = self.update_debtpaying_data ) )
		list_process.append( Process( target = self.update_cashflow_data ) )
		list_process.append( Process( target = self.update_forcast_quarter_report_data ) )
		list_process.append( Process( target = self.update_k_line_data ) )
		list_process.append( Process( target = self.update_divi_data ) )
		list_process.append( Process( target = self.update_restrict_stock_data ) )
		list_process.append( Process( target = self.update_concept_classified ) )		
		
		for process in list_process:
			process.start()
		for process in list_process:
			process.join()	
	'''--------------- custom query func ---------------'''	
	@Utils.func_timer
	def get_all_stock_data( self ):

		df_stock_basics = self.get_stock_basics()
		df_stock_basics = df_stock_basics.set_index( 'code' )

		df_quarter_report_data = self.get_quarter_report_data()
		df_quarter_report_data = df_quarter_report_data.set_index( 'code' )

		df_profit_data = self.get_profit_data()
		df_profit_data = df_profit_data.set_index( 'code' )
		
		df_operation_data = self.get_operation_data()
		df_operation_data = df_operation_data.set_index( 'code' )
		
		df_growth_data = self.get_growth_data()
		df_growth_data = df_growth_data.set_index( 'code' )
		
		df_debtpaying_data = self.get_debtpaying_data()
		df_debtpaying_data = df_debtpaying_data.set_index( 'code' )
		
		df_cashflow_data = self.get_cashflow_data()
		df_cashflow_data = df_cashflow_data.set_index( 'code' )
		
		df_divi_data = self.get_divi_data()
		df_divi_data = df_divi_data.set_index( 'code' )
		
		df_forcast_quarter_report_data = self.get_forcast_quarter_report_data()
		df_forcast_quarter_report_data = df_forcast_quarter_report_data.set_index( 'code' )

		df_restrict_stock_data = self.get_restrict_stock_data()
		df_restrict_stock_data = df_restrict_stock_data.set_index( 'code' )
		
		df_concept_classified = self.get_concept_classified_data()
		df_concept_classified = df_concept_classified.set_index( 'code' )

		return [ df_stock_basics, df_quarter_report_data, df_profit_data, df_operation_data, df_growth_data, df_debtpaying_data, \
				df_cashflow_data, df_divi_data, df_forcast_quarter_report_data, df_restrict_stock_data, df_concept_classified ]

	@Utils.func_timer
	def query_stock_info( self, code ):

		[ df_stock_basics, df_quarter_report_data, df_profit_data, df_operation_data, df_growth_data, df_debtpaying_data, \
			df_cashflow_data, df_divi_data, df_forcast_quarter_report_data, df_restrict_stock_data, df_concept_classified ] = \
			self.get_all_stock_data()
		content = ''
		space = lambda x : ' ' * x # 方便区分不同季度数据		
		try:
			basics = df_stock_basics.loc[ int( code ) ]
			cur_price = float( self.get_realtime_quotes( code )[ 'price' ] )

			content += 'basics:\n上市日期：{0}\n所属行业：{1}\n地区：{2}\n市盈率(动态)：{3}\n市盈率(静态)：{4:.2f}\n市净率：{5}\n'\
				.format( basics[ 'timeToMarket' ], basics[ 'industry' ], basics[ 'area' ], basics[ 'pe' ], \
						cur_price / float( basics[ 'esp' ] ), float( basics[ 'pb' ] ) )
			content += '每股公积金：{0}\n每股未分配利润：{1}\n'\
				.format( basics[ 'reservedPerShare' ], basics[ 'perundp' ] )
			content += '总市值：{0:.2f} 亿元\n流动市值：{1:.2f} 亿元\n'\
				.format( cur_price * float( basics[ 'totals' ] ), cur_price * float( basics[ 'outstanding' ] ) )
			content += '总资产：{0:.2f} 亿元\n固定资产：{1:.2f} 亿元\n流动资产：{2:.2f} 亿元\n'\
				.format( float( basics[ 'totalAssets' ] ) / 10000, float( basics[ 'fixedAssets' ] ) / 10000, \
						float( basics[ 'liquidAssets' ] ) / 10000 )
		except: pass

		try:
			concept = df_concept_classified.loc[ int( code ) ]

			content += '\nconcept:\n'
			for id in range( concept.index.size ):
				content += '{0}\n'.format( concept.iloc[ id ][ 'c_name' ] )
		except: pass

		try: 
			profit = df_profit_data.loc[ int( code ) ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True ).drop_duplicates()
			content += '\nprofit:\n年份   季度  净资产收益率  净利润（万元）  每股收益（元）\n'
			for id in range( profit.index.size ):
				content += '{5}{0}  {1}  {2:-10.2f}  {3:-12.2f}  {4:-15.2f}\n'.format( profit.iloc[ id ][ 'year' ], profit.iloc[ id ][ 'quarter' ], \
						profit.iloc[ id ][ 'roe' ], profit.iloc[ id ][ 'net_profit_ratio' ], profit.iloc[ id ][ 'eps' ], \
						space( int( profit.iloc[ id ][ 'quarter' ] ) - 1 ) )
		except: pass

		try: 
			operation = df_operation_data.loc[ int( code ) ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True ).drop_duplicates()
			content += '\noperation:\n年份   季度  应收账款周转天数  存货周转天数  流动资产周转天数\n'
			
			for id in range( operation.index.size ):
				content += '{5}{0}  {1}  {2:-16.2f}     {3:-8.2f}     {4:-15.2f}\n'.format( operation.iloc[ id ][ 'year' ], \
					operation.iloc[ id ][ 'quarter' ],operation.iloc[ id ][ 'arturndays' ], operation.iloc[ id ][ 'inventory_days' ], \
					operation.iloc[ id ][ 'currentasset_days' ], space( int( operation.iloc[ id ][ 'quarter' ] ) - 1 ) )
		except: pass
			
		try:		
			debtpaying = df_debtpaying_data.loc[ int( code ) ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True ).drop_duplicates()
			content += '\ndebtpaying:\n年份   季度  流动比率  利息支付倍数  股东权益比率  股东权益增长率\n'

			for col in [ 'currentratio', 'icratio', 'sheqratio', 'adratio' ]:
				for id in range( debtpaying.index.size ):
					try:
						float( debtpaying[ col ].iloc[ id ] )
					except:
						debtpaying[ col ].iloc[ id ] = np.nan
			
			for id in range( debtpaying.index.size ):
					content += '{5}{0}  {1}  {2:-8.2f}   {3:-12.2f}   {4:-10.2f}   {6:-14.2f}\n'.format( debtpaying.iloc[ id ][ 'year' ], \
						debtpaying.iloc[ id ][ 'quarter' ], float( debtpaying.iloc[ id ][ 'currentratio' ] ), float( debtpaying.iloc[ id ][ 'icratio' ] ), \
						float( debtpaying.iloc[ id ][ 'sheqratio' ] ), space( int( debtpaying.iloc[ id ][ 'quarter' ] ) - 1 ), \
						float( debtpaying.iloc[ id ][ 'adratio' ] ) )
		except: pass

		try: 
			divi = df_divi_data.loc[ int( code ) ].sort_values( by = 'year', axis = 0, ascending = True )
			content += '\ndivision:\n年份    公布日期  分红金额(每10股)  转增股数(每10股)\n'

			for id in range( divi.index.size ):
				content += '{0}  {1}  {2:-12d}  {3:-16d}\n'.format( divi.iloc[ id ][ 'year' ], divi.iloc[ id ][ 'report_date' ], int( divi.iloc[ id ][ 'divi' ] ), \
					int( divi.iloc[ id ][ 'shares' ] ) )
		except: pass

		try:
			focast_quarter_data = df_forcast_quarter_report_data.loc[ int( code ) ].sort_values( by = 'report_date', axis = 0, ascending = True )
			content += '\nforcast quarter report:\n发布日期    业绩变动类型  上年同期每股收益  业绩变动范围\n'

			for id in range( focast_quarter_data.index.size ):
				content += '{0}  {1:>8s}  {2:-14.2f}  {3:>12s}\n'.format( focast_quarter_data.iloc[ id ][ 'report_date' ], \
					focast_quarter_data.iloc[ id ][ 'type' ], float( focast_quarter_data.iloc[ id ][ 'pre_eps' ] ), \
					focast_quarter_data.iloc[ id ][ 'range' ] )
		except: pass

		# try:
		restrict = df_restrict_stock_data.loc[ int( code ) ].sort_values( by = 'date', axis = 0, ascending = True )
		content += '\nrestrict:\n解禁日期    解禁数量（万股）  占总盘比率\n'

		for id in range( restrict.index.size ):
			content += '{0}  {1:-12.2f}  {2:-10.2f}\n'.format( restrict.iloc[ id ][ 'date' ], \
					float( restrict.iloc[ id ][ 'count' ] ), float( restrict.iloc[ id ][ 'ratio' ] ) )
		# except: pass

		print( content )
					
'''--------------- run ---------------'''			
if __name__ == '__main__':
	
	# Data( Utils.cur_date() ).update_all()
	code = '300299'
	Data().query_stock_info( code )