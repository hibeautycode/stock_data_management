import sys, os, time, threading, datetime
sys.path.append( '../utils' )
from utils import Utils, SAVE_DATA, LOG, ERROR
import tushare as ts
import pandas as pd
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
			self.update_growth_data()
			Utils.save_data( df_profit_data, self.__profit_file_name, 'quarter report' )
	
	def get_profit_data( self ):
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
						ERROR( 'exception occurs when update quarter report data of year {0} quarter {1}'.format( year, quarter ) )
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

'''--------------- run ---------------'''			
if __name__ == '__main__':
	
	Data( Utils.cur_date() ).update_all()
		