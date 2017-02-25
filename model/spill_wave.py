import sys, os, threading, re
sys.path.append( '../data' )
sys.path.append( '../utils' )
sys.path.append( '../trade' )
from data import Data
from utils import Utils, SAVE_DATA, LOG, ERROR, SEND_EMAIL
from trade import Position
from pandas import DataFrame, Series
from numpy import arange
from multiprocessing import Queue, Process
from time import sleep

class Spill_Wave():

# 凹波形参数：下降阈值 min_descend_rate
			# 上升阈值 growth_rate
			# 在max_last_days天数内找出最高点 并计算整体收益期望和方差（风险）
			# num_refer_pre_days 参考之前num_refer_pre_days天内的股价，如果当前股价高于之前平均，则视为当前spill不满足条件
			#  参考之前num_refer_pre_days天内的最小股价，如果当前股价比最小股价高出超过rate_price2_higher_than_min，则视为当前spill不满足条件
			# df_spill 存储每个满足条件的波形参数及delay_days内最高价格:
				# col=('code','crest date','through date','buy date','sell date','crest price','through price',
					# 'buy price','sell price','descend rate','best earn rate')

	def __init__( self, growth_rate = 0.05, max_last_days = 40, num_refer_pre_days = 60, rate_price2_higher_than_min = 0.2, min_descend_rate = 0.08 ):
	
		self.min_descend_rate = min_descend_rate
		self.growth_rate = growth_rate
		self.max_last_days = max_last_days
		self.num_refer_pre_days = num_refer_pre_days
		self.rate_price2_higher_than_min = rate_price2_higher_than_min
		self.NUM_SPLIT_SPILL = 30
		self.num_append_spiil_stock = 0
		self.list_df_split_spill = []
		self.list_path_file = []
		
		for i in range( self.NUM_SPLIT_SPILL ):
			self.list_df_split_spill.append( DataFrame( columns = ( 'code', 'crest date', 'through date', 'buy date', \
											'sell date', 'crest price', 'through price', 'buy price' , 'sell price', \
											'descend rate', 'best earn rate' ) ) )
		
		if SAVE_DATA == 'xls':
			self.path_result = '../model/spill_wave/wave_' + str( self.min_descend_rate ) + '_' + str( self.growth_rate ) \
							+ '_' + str( self.max_last_days ) + '_' + str( self.num_refer_pre_days ) + '_' + str( self.rate_price2_higher_than_min )
			for i in range( self.NUM_SPLIT_SPILL ):
				self.list_path_file.append( self.path_result + '/'+ '(' + str( i ) + ')' + '.xlsx' )
	
	def append_spill_list( self, code, split_id, mutex_lock_list_df_split_spill = None, filter_pre_days = 5 ):
	
		k_data = Data().get_k_line_data( code )
		
		num_market_days = k_data.index.size
		if num_market_days <= filter_pre_days:
			return
			
		#price_list -- first point: crest point; second point: through point; third point: meet growth_rate point
		price_list = [ k_data.get( 'high' )[ filter_pre_days ], k_data.get( 'low' )[ filter_pre_days ], k_data.get( 'low' )[ filter_pre_days ] ]
		date_list = [ k_data.get( 'date' )[ filter_pre_days ], k_data.get( 'date' )[ filter_pre_days ], k_data.get( 'date' )[ filter_pre_days ], '' ]
		flag_meet_descend_rate = False;	flag_meet_growth_rate = False
		
		for i in range( filter_pre_days, num_market_days ):
		
			if flag_meet_descend_rate and flag_meet_growth_rate:
			# 找到max_last_days天内的最大卖出价格，将满足条件的spill插入DataFrame
				max_value = k_data.get( 'high' )[ i ]
				date_list[ 3 ] = k_data.get( 'date' )[ i ]
				max_days = min( num_market_days - i, self.max_last_days )
				
				for j in range( i, i + max_days ):
				# 找到max_last_days天内的最大卖出价格
					if k_data.get( 'high' )[ j ] > max_value:
						max_value = k_data.get( 'high' )[ j ]
						date_list[ 3 ] = k_data.get( 'date' )[ j ]
				
				if max_days == self.max_last_days:
				# 将满足条件的spill插入DataFrame
					if mutex_lock_list_df_split_spill is not None:
						mutex_lock_list_df_split_spill.acquire()
					self.list_df_split_spill[ split_id ].loc[ self.list_df_split_spill[ split_id ].index.size ] = [ code, date_list[ 0 ], date_list[ 1 ], date_list[ 2 ], date_list[ 3 ], \
											'%.2f' % price_list[ 0 ], '%.2f' % price_list[ 1 ], '%.2f' % price_list[ 2 ], \
											'%.2f' % max_value, '%.4f' % ( ( price_list[ 0 ] - price_list[ 1 ] ) / price_list[ 0 ] ), \
											'%.4f' % ( ( max_value - price_list[ 2 ] ) / price_list[ 2 ] ) ]
					if mutex_lock_list_df_split_spill is not None:
						mutex_lock_list_df_split_spill.release()
				
				price_list = [ k_data.get( 'high' )[ i ], k_data.get( 'low' )[ i ], k_data.get( 'low' )[ i ] ]
				date_list = [ k_data.get( 'date' )[ i ], k_data.get( 'date' )[ i ], k_data.get( 'date' )[ i ], '' ]
				flag_meet_descend_rate = False;	flag_meet_growth_rate = False
				
			elif flag_meet_descend_rate and not flag_meet_growth_rate:
			# 现波形已经满足下降阈值，找到满足回升阈值的波形
				if k_data.get( 'low' )[ i ] < price_list[ 1 ]:
					price_list[ 1 ] = price_list[ 2 ] = k_data.get( 'low' )[ i ]
					date_list[ 1 ] = date_list[ 2 ] = k_data.get( 'date' )[ i ]
				elif k_data.get( 'high' )[ i ] > price_list[ 2 ]:
					price_list[ 2 ] = k_data.get( 'high' )[ i ]
					date_list[ 2 ] = k_data.get( 'date' )[ i ]
				if ( price_list[ 2 ] - price_list[ 1 ] ) / price_list[ 1 ] >= self.growth_rate:
					price_list[ 2 ] = price_list[ 1 ] * ( 1 + self.growth_rate )
					flag_meet_growth_rate = True
					
					# 参考之前num_refer_pre_days天内的股价，如果当前股价过高，则视为当前spill不满足条件
					tmp_refer_pre_days = min( i, self.num_refer_pre_days )
					average_price = 0
					pre_min_price = 500 
					
					for k in range( i - tmp_refer_pre_days, i ):
						cur_price = k_data.get( 'low' )[ k ]
						average_price += cur_price
						if cur_price < pre_min_price:
							pre_min_price = cur_price
					average_price /= tmp_refer_pre_days
					
					if price_list[ 2 ] >= average_price or ( price_list[ 2 ] - pre_min_price ) / pre_min_price >= self.rate_price2_higher_than_min:
						price_list = [ k_data.get( 'high' )[ i ], k_data.get( 'low' )[ i ], k_data.get( 'low' )[ i ] ]
						date_list = [ k_data.get( 'date' )[ i ], k_data.get( 'date' )[ i ], k_data.get( 'date' )[ i ], '' ]
						flag_meet_descend_rate = False;	flag_meet_growth_rate = False						
				
			elif not flag_meet_descend_rate and not flag_meet_growth_rate:
			# 找到满足下降阈值的波形
				if k_data.get( 'high' )[ i ] > price_list[ 0 ]:
					price_list[ 0 ] = k_data.get( 'high' )[ i ];	price_list[ 1 ] = k_data.get( 'low' )[ i ]
					date_list[ 0 ] = k_data.get( 'date' )[ i ]; date_list[ 1 ] = k_data.get( 'date' )[ i ]
				elif k_data.get( 'low' )[ i ] < price_list[ 1 ]:
					price_list[ 1 ] = k_data.get( 'low' )[ i ]
					date_list[ 1 ] = k_data.get( 'date' )[ i ]
				if ( price_list[ 0 ] - price_list[ 1 ] ) / price_list[ 0 ] >= self.min_descend_rate:
					flag_meet_descend_rate = True
					price_list[ 2 ] = price_list[ 1 ]
					date_list[ 2 ] = date_list[ 1 ]
	
	def get_all_spill( self, num_threads = 8 ):
		
		def get_part_spill( self, df_stock_basics, id_start, id_end, mutex_lock_stock_num,  mutex_lock_list_df_split_spill ):
				
			for i in range( id_start, id_end ):	
				code = '%06d' % int( df_stock_basics['code'][i] )
				mutex_lock_stock_num.acquire()
				self.num_append_spiil_stock += 1
				mutex_lock_stock_num.release()
				LOG( 'pid-{4} {3} get {0} spill data {1:4d}/{2:4d} min_descend_rate:{5} growth_rate:{6} '.format( code, self.num_append_spiil_stock, num_stock, threading.current_thread().getName(), os.getpid(), self.min_descend_rate, self.growth_rate ) )
				self.append_spill_list( code, i % self.NUM_SPLIT_SPILL, mutex_lock_list_df_split_spill )
		
		df_tmp_stock_basics = Data().get_stock_basics()
		num_stock = df_tmp_stock_basics.index.size
		
		list_threads = []
		mutex_lock_list_df_split_spill = threading.Lock()
		mutex_lock_stock_num = threading.Lock()
		for n in range( num_threads ):
			list_threads.append( threading.Thread( target = get_part_spill, \
				args=( self, df_tmp_stock_basics, int( num_stock / num_threads ) * n, int( num_stock / num_threads ) * ( n + 1 ), mutex_lock_stock_num, mutex_lock_list_df_split_spill ) ) )
		list_threads.append( threading.Thread( target = get_part_spill, \
			args=( self, df_tmp_stock_basics, int( num_stock / num_threads ) * num_threads, num_stock, mutex_lock_stock_num, mutex_lock_list_df_split_spill ) ) )
		
		for thread in list_threads:
			thread.start()
		for thread in list_threads:
			thread.join()
		
	def save_data( self ):
	
		if not os.path.exists( self.path_result ):
			os.mkdir( self.path_result )
		
		for i in range( self.NUM_SPLIT_SPILL ):
			Utils.save_data( self.list_df_split_spill[ i ], self.list_path_file[ i ] )
			
	def analyse_spill_data( self, list_descend_rate_interval, process_queue, filter_singular_rate = 0.2 ):
	
		list_df_split_spill = []
		series_earn_rate = Series()		
		
		for n in range( self.NUM_SPLIT_SPILL ):
			df_tmp = Utils.read_data( self.list_path_file[ n ] )			
			if df_tmp is None:
				list_df_split_spill = []
				self.get_all_spill()
				self.save_data()
				for n in range( self.NUM_SPLIT_SPILL ):
					df_tmp = Utils.read_data( self.list_path_file[ n ] )
					if df_tmp is not None:
						list_df_split_spill.append( df_tmp )
					else:
						ERROR( 'spill data [{0}] not exists'.format( n ) )
				break
			else: 
				list_df_split_spill.append( df_tmp )
				
		for n in range( self.NUM_SPLIT_SPILL ):
			list_df_split_spill[ n ] = list_df_split_spill[ n ][ list_df_split_spill[ n ][ 'descend rate' ] >= list_descend_rate_interval[ 0 ] ]
			list_df_split_spill[ n ] = list_df_split_spill[ n ][ list_df_split_spill[ n ][ 'descend rate' ] < list_descend_rate_interval[ 1 ] ]
				
		for n in range( self.NUM_SPLIT_SPILL ):
			series_earn_rate = series_earn_rate.append( list_df_split_spill[ n ][ 'best earn rate' ] )
		
		# 滤出收益率过大奇点
		filter_singular_num = int( series_earn_rate.size * filter_singular_rate )
		series_earn_rate = series_earn_rate.sort_values( ascending = False )[ filter_singular_num: ]
		expect_earn_rate = series_earn_rate.mean()
		min_earn_rate = series_earn_rate.min()
		
		list_earn_rate = series_earn_rate.tolist()
		variance_earn_rate = 0
		for i in range( len( list_earn_rate ) ):
			variance_earn_rate += list_earn_rate[ i ] ** 2
		try:
			variance_earn_rate = variance_earn_rate / len( list_earn_rate ) - expect_earn_rate ** 2
		except:
			LOG( 'pid-{2} spill not exists when min_descend_rate:{0} growth_rate:{1}'.format( self.min_descend_rate, self.growth_rate, os.getpid() ) )
		
		list_res = [ [ '%.2f' % list_descend_rate_interval[ 0 ], '%.2f' % list_descend_rate_interval[ 1 ] ], self.growth_rate, \
					'%.4f' % expect_earn_rate, '%.4f' % min_earn_rate, '%.4f' % variance_earn_rate ]
		process_queue.put( list_res )
		LOG( '{4} pid-{0} finish analyzing. descend_rate_interval:[ {1:.3f} {2:.3f} ] growth_rate:{3:.3f}'.format( os.getpid(), list_descend_rate_interval[ 0 ], list_descend_rate_interval[ 1 ], self.growth_rate, Utils.cur_time() ) ) 

class Analyse():

	def __init__( self, file_date ):
	
		self.analyse_result_path = '../model/analyse_result/'
		self.num_check_stock = 0
		self.descend_rate_interval_len = 0.01
		self.growth_rate_interval_len = 0.01
		self.min_descend_rate = 0.08
		self.min_growth_rate = 0.02
		self.max_descend_rate = 0.5 + 0.001
		self.max_growth_rate = 0.15 + 0.001
		self.max_last_days = 40
		self.num_refer_pre_days = 60
		self.rate_price2_higher_than_min = 0.2

		self.value_stock_file = self.analyse_result_path + 'value_stock(spill_wave)_' + file_date + '.xls'
		self.analyse_spill_wave_file = self.analyse_result_path + 'spill_wave_' + str( self.min_descend_rate ) + '_' \
							+ str( self.descend_rate_interval_len ) + '_' + str( self.max_last_days ) + '_' + \
							str( self.num_refer_pre_days ) + '_' + str( self.rate_price2_higher_than_min ) + '.xls'
		self.df_value_stock = DataFrame( columns = ( 'code', 'name', 'buy_price', 'sell_price', 'descend_rate', \
												'growth_rate', 'expect_earn_rate', 'min_earn_rate', 'variance_earn_rate' ) )
		self.num_refer_pre_days = 60
		self.rate_price0_higher_than_min = 0.2
	
	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.analyse_result_path ):
			os.mkdir( self.analyse_result_path )
		Utils.save_data( df_data, file_path_name )
		
	@Utils.func_timer		
	def train( self ):	
		
		df_res = DataFrame( columns = ( 'descend_rate_interval', 'growth_rate', 'expect_earn_rate', 'min_earn_rate', 'variance_earn_rate' ) )
			
		process_queue = Queue( maxsize = 0 )	# 队列最大长度设置为最大可用长度
		list_process = []
		
		for descend_rate in arange( self.min_descend_rate, self.max_descend_rate, self.descend_rate_interval_len ):
			for growth_rate in arange( self.min_growth_rate, self.max_growth_rate, self.growth_rate_interval_len ):
				spill_class = Spill_Wave( float( '%.3f' % growth_rate ), self.max_last_days, self.num_refer_pre_days, self.rate_price2_higher_than_min, self.min_descend_rate )
				descend_rate1 = descend_rate
				descend_rate2 = descend_rate + self.descend_rate_interval_len
				list_interval = [ descend_rate1, descend_rate2 ]
				if descend_rate >= self.max_descend_rate - 0.002 and descend_rate <= self.max_descend_rate:
					list_interval = [ descend_rate1, 1.0 ]
				list_process.append( Process( target = spill_class.analyse_spill_data, args = ( list_interval, process_queue, ) ) )
				if len( list_process ) == 10:
					for process in list_process:
						process.start()
					for process in list_process:
						process.join()	
					list_process = []
					# CPUs have a rest for 60s
					num_minutes = 0
					LOG( '{0} resting for {1} minutes…'.format( Utils.cur_time(), num_minutes ) )
					while not process_queue.empty():
						df_res.loc[ df_res.index.size ] = process_queue.get()
					sleep( num_minutes * 60 )
		
		for process in list_process:
			process.start()
		for process in list_process:
			process.join()	
		
		while not process_queue.empty():
			df_res.loc[ df_res.index.size ] = process_queue.get()
				
		self.save_data( df_res, self.analyse_spill_wave_file )
	
	def append_value_stock( self, code, name,  mutex_lock_df_value_stock ):
	
		k_data = Data().get_k_line_data( code )
		filter_pre_days = 10		
		num_market_days = k_data.index.size
		best_expect_earn_rate = 0
		variance_earn_rate = 0
		flag_value_stock = False
		list_res = []
				
		if num_market_days < filter_pre_days:
			return
		
		df_spill_analyse_res = Utils.read_data( self.analyse_spill_wave_file )
		
		for growth_rate in arange( self.min_growth_rate, self.max_growth_rate, self.growth_rate_interval_len ):
			growth_rate = float( '%.3f' % growth_rate )
			#price_list -- first: current highest price; second: stage through price; third: stage growth_rate price
			price_list = [ k_data.get( 'high' )[ num_market_days - 1 ], k_data.get( 'low' )[ num_market_days - 1 ], \
					k_data.get( 'low' )[ num_market_days - 1 ] ]
			for i in range( num_market_days - 2, -1, -1 ):
				
				if ( price_list[ 0 ] - price_list[ 1 ] ) / price_list[ 1 ] > growth_rate:
				# 当前价格已经超过模型增长率上限，视此为无价值股
					break
					
				descend_rate = ( price_list[ 2 ] - price_list[ 1 ] ) / price_list[ 2 ]
				
				if ( price_list[ 2 ] - k_data.get( 'high' )[ i ] ) / k_data.get( 'high' )[ i ] >= growth_rate:
				# 当前拐点已超过增长率上限（波形下降率判定截止条件）
					if descend_rate >= self.min_descend_rate:
					# 下降率满足波形条件,记录波形参数
						df_tmp = df_spill_analyse_res[ df_spill_analyse_res.growth_rate == growth_rate ]
						pattern = re.compile( '[\[\]\'\,]+' )
						expect_earn_rate = 0
						min_earn_rate = 0
						for id in df_tmp.index:
						# find related expect_earn_rate
							descend_rate_interval_0 = float( re.split( pattern, df_tmp.loc[ id ][ 'descend_rate_interval' ])[ 1 ] )
							descend_rate_interval_1 = float( re.split( pattern, df_tmp.loc[ id ][ 'descend_rate_interval' ])[ 3 ] )
							
							if descend_rate >= descend_rate_interval_0 and descend_rate < descend_rate_interval_1:
								expect_earn_rate = df_tmp.loc[ id ]['expect_earn_rate']
								min_earn_rate = df_tmp.loc[ id ]['min_earn_rate']
								variance_earn_rate = df_tmp.loc[ id ]['variance_earn_rate']
								break
						
						
						if expect_earn_rate > best_expect_earn_rate:
							flag_value_stock = True	
							best_expect_earn_rate = expect_earn_rate							
							buy_price = price_list[ 1 ] * ( 1 + growth_rate )
							sell_price = buy_price * ( 1 + best_expect_earn_rate )
							list_res = [ code, name, '%.2f' % buy_price, '%.2f' % sell_price, '%.3f' % descend_rate, \
										growth_rate, float( '%.3f' % expect_earn_rate ), float( '%.3f' % min_earn_rate ), \
										'%.3f' % variance_earn_rate ]
										
							# 参考之前num_refer_pre_days天内的股价，如果当前股价过高，则视为当前spill不满足条件，与模型对应
							tmp_refer_pre_days = min( i, self.num_refer_pre_days )
							average_price = 0
							pre_min_price = 500 
							
							for k in range( i - tmp_refer_pre_days, i ):
								cur_price = k_data.get( 'low' )[ k ]
								average_price += cur_price
								if cur_price < pre_min_price:
									pre_min_price = cur_price					
							
							if tmp_refer_pre_days == 0:
								average_price = price_list[ 2 ]
							else:
								average_price /= tmp_refer_pre_days
								
							if price_list[ 0 ] >= average_price or \
								( price_list[ 0 ] - pre_min_price ) / pre_min_price >= self.rate_price0_higher_than_min:
							# 当前价格较高
								flag_value_stock = False
						break
					else:
						break
					
				if k_data.get( 'low' )[ i ] < price_list[ 1 ]:
					if ( price_list[ 2 ] - k_data.get( 'low' )[ i ] ) / k_data.get( 'low' )[ i ] >= growth_rate:
					# 当前统计波形的最高点已经超过模型增长率筛选条件
						break
					else:
						price_list[ 1 ] = k_data.get( 'low' )[ i ]
					
				if k_data.get( 'high' )[ i ] > price_list[ 2 ]:
					price_list[ 2 ] = k_data.get( 'high' )[ i ]
					
		if flag_value_stock:
			mutex_lock_df_value_stock.acquire()
			self.df_value_stock.loc[ self.df_value_stock.index.size ] = list_res
			mutex_lock_df_value_stock.release()
		
	@Utils.func_timer
	def find_value_stock( self, num_threads = 8 ):
	
		def find_part_value_stock( self, df_stock_basics, id_start, id_end, mutex_lock_stock_num,  mutex_lock_df_value_stock ):
				
			for i in range( id_start, id_end ):	
				code = '%06d' % int( df_stock_basics['code'][i] )
				name = df_stock_basics['name'][i]
				mutex_lock_stock_num.acquire()
				self.num_check_stock += 1
				LOG( 'pid-{4} {3} check {0} ? value stock {1:4d}/{2:4d} '.\
					format( code, self.num_check_stock, num_stock, threading.current_thread().getName(), os.getpid() ) )
				mutex_lock_stock_num.release()
				self.append_value_stock( code, name, mutex_lock_df_value_stock )
		
		df_tmp_stock_basics = Data().get_stock_basics()
		num_stock = df_tmp_stock_basics.index.size
		
		list_threads = []
		mutex_lock_df_value_stock = threading.Lock()
		mutex_lock_stock_num = threading.Lock()
		for n in range( num_threads ):
			list_threads.append( threading.Thread( target = find_part_value_stock, args = ( self, df_tmp_stock_basics, \
				int( num_stock / num_threads ) * n, int( num_stock / num_threads ) * ( n + 1 ), \
				mutex_lock_stock_num, mutex_lock_df_value_stock ) ) )
		list_threads.append( threading.Thread( target = find_part_value_stock, \
			args=( self, df_tmp_stock_basics, int( num_stock / num_threads ) * num_threads, \
			num_stock, mutex_lock_stock_num, mutex_lock_df_value_stock ) ) )
		
		for thread in list_threads:
			thread.start()
		for thread in list_threads:
			thread.join()
		
		self.df_value_stock = self.df_value_stock.sort_values( by = 'expect_earn_rate', axis = 0, ascending = False )
		self.save_data( self.df_value_stock, self.value_stock_file )
	
	def notify_investment_opportunity( self ):
		
		df_value_stock = Utils.read_data( self.value_stock_file )
		
		while True:
		
			cur_time = Utils.cur_time()
			hour = int( cur_time.split( ':' )[ 0 ] )
			minute = int( cur_time.split( ':' )[ 1 ] )
			
			if hour < 9 or ( hour == 9 and minute < 30 ) or ( hour == 11 and minute >= 30 ) or hour == 12:
				LOG( 'market not open from notify_investment_opportunity' )
				sleep( 60 )
				continue
			elif hour >= 15:
				LOG( 'market close from notify_investment_opportunity' )
				break
			content_notify = ''
			LOG( '*********************************' )
			LOG( '{0}'.format( cur_time ) )
			
			content_notify += '{0}\n'.format( cur_time )
			for index in df_value_stock.index:
				code = '%06d' % df_value_stock.loc[ index ][ 'code' ]
				name = df_value_stock.loc[ index ][ 'name' ]
				try:
					df_realtime_quotes = Data().get_realtime_quotes( code )				
				
					if float( df_realtime_quotes[ 'price' ] ) >= ( float( df_value_stock.loc[ index ][ 'buy_price' ] ) * 0.99 ) :
						LOG( '{0}  {1}  cur:{2:.2f}  buy:{3:.2f}  earn:{4:.2f}'\
							.format( code, name, float( df_realtime_quotes[ 'price' ] ), \
									float( df_value_stock.loc[ index ][ 'buy_price' ] ), \
									float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ), \
									float( df_value_stock.loc[ index ][ 'min_earn_rate' ] ) ) )
						content_notify += '-{0}  {1}  cur:{2:.2f}  buy:{3:.2f}  earn:{4:.2f}\n'\
							.format( code, name, float( df_realtime_quotes[ 'price' ] ), \
									float( df_value_stock.loc[ index ][ 'buy_price' ] ), \
									float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ), \
									float( df_value_stock.loc[ index ][ 'min_earn_rate' ] ) )	
				except:
					pass								
			LOG( '*********************************' )
			if SEND_EMAIL:
			# 如果发送邮件，10分钟发一次
				Utils.send_email( content_notify, 'opportunity notification' )
				sleep( 10 * 60 )
			else:
				sleep( 120 )
				
if __name__ == '__main__':

	if int( Utils.cur_time().split( ':' )[ 0 ] ) < 18:
	# 18点之前，采用前一天的分析结果
		file_date = Utils.last_date()
		analyse_class = Analyse( file_date )
	else:
	# 当日18点之后方可更新数据	
		file_date = Utils.cur_date()
		Data( file_date ).update_all()
		analyse_class = Analyse( file_date )
		#analyse_class.train()
		analyse_class.find_value_stock()
		
	list_process = []
	list_process.append( Process( target = analyse_class.notify_investment_opportunity ) )
	list_process.append( Process( target = Position().notify_realtime_earnings ) )
	list_process.append( Process( target = Position().serve_query_request() ) )
	
	for process in list_process:
		process.start()
	for process in list_process:
		process.join()
	
	