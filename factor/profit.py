import numpy as np
import sys, os, datetime, threading
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
from pandas import DataFrame
from multiprocessing import Queue, Process

class Profit():

	def __init__( self ):

		self.result_path = '../factor/result/'
		self.df_profit_grow = DataFrame( columns = ( 'code', 'name', 'profit_grow' ) )
		self.profit_grow_file = self.result_path + 'profit_grow.xlsx'

	def sub_calc_profit_grow( self, list_code, id_start, id_end, df_profit_data, df_stock_basics, queue_process ):

		tmp_ls = []

		for id in range( id_start, id_end ):
			code = list_code[ id ]
			tmp_df =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
			code = '%06d' % code		
			try:	
				cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
			except:
				# 尚没有k线的股票跳过
				continue
			
			basics = df_stock_basics.loc[ int( code ) ]
			ls_grow_ratio = []

			for i in range( tmp_df.index.size - 4 ):

				if tmp_df.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 5:
				# 统计近4到5年的数据
					if tmp_df.iloc[ i + 4 ][ 'quarter' ] == tmp_df.iloc[ i ][ 'quarter' ] and \
						( tmp_df.iloc[ i + 4 ][ 'year' ] - tmp_df.iloc[ i ][ 'year' ] ) == 1:
						
						if np.isnan( tmp_df.iloc[ i ][ 'net_profits' ] ) or np.isnan( tmp_df.iloc[ i + 4 ][ 'net_profits' ] ):
							continue

						ls_grow_ratio.append( ( tmp_df.iloc[ i + 4 ][ 'net_profits' ] - tmp_df.iloc[ i ][ 'net_profits' ] ) \
												/ ( abs( tmp_df.iloc[ i ][ 'net_profits' ] ) + abs( tmp_df.iloc[ i + 4 ][ 'net_profits' ] ) + 1 ) )
					
			if len( ls_grow_ratio ) == 0:
				continue
			ls_weight = []

			# 指数赋权
			for i in range( len( ls_grow_ratio ) ):
				if ls_grow_ratio[ i ] < 0:
					# 利润增长下降惩罚因子
					eta = 1.0
					ls_weight.append( eta * np.exp( 1 - i * 0.25 ) )
				else:
					ls_weight.append( np.exp( 1 - i * 0.25 ) )
			# for i in range( len( ls_grow_ratio ) ):
			# 	ls_weight.append( 10 - ( 0.1 * i ) )

			ls_weight.reverse()
			arr_weight = np.array( ls_weight ) / sum( np.array( ls_weight ) )
			arr_grow_ratio = np.array( ls_grow_ratio )

			# profit_grow = 100 * ( 利润增长数组 · 权重数组 ) / 市盈率
			# profit_grow = 100.0 * ( 1.0 + float( np.dot( np.array( ls_grow_ratio ), arr_weight ) ) ) * float( basics[ 'esp' ] ) / cur_price
			profit_grow = 50.0 * ( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ) ) / ( 1.0 + float( np.var( arr_grow_ratio ) ) ) # 方差：震荡因子
			LOG( '{0} {1} {2}'.format( code, profit_grow, float( np.var( arr_grow_ratio ) ) ) )

			profit_grow = float( '{0:.2f}'.format( profit_grow ) )
			name = tmp_df.iloc[ 0 ][ 'name' ]

			tmp_ls.append( [ code, name, profit_grow ] )
		queue_process.put_nowait( tmp_ls )
		
	@Utils.func_timer
	def calc_profit_grow( self, num_process = 30 ):
		
		df_profit_data = Data().get_profit_data()
		df_profit_data = df_profit_data.set_index( 'code' ).drop_duplicates()

		df_stock_basics = Data().get_stock_basics()
		df_stock_basics = df_stock_basics.set_index( 'code' )

		set_code = set()
		for code in df_profit_data.index:
			set_code.add( code )

		list_code = list( set_code )
		num_stock = len( list_code )

		list_process = []
		queue_process = Queue()

		for n in range( num_process ):
			list_process.append( Process( target = self.sub_calc_profit_grow, \
				args=( list_code, int( num_stock  / num_process ) * n, int( num_stock / num_process ) * ( n + 1 ), \
					df_profit_data, df_stock_basics, queue_process ) ) )

			if len( list_process ) == 10:

				for process in list_process:
					process.start()
				for process in list_process:
					process.join()	

				list_process = []	

				while not queue_process.empty():			
					LOG( 'queue_process:{0}'.format( queue_process.qsize() ) )
					ls_get = queue_process.get()
					for ls in ls_get:
						self.df_profit_grow.loc[ self.df_profit_grow.index.size ] = ls

		list_process.append( Process( target = self.sub_calc_profit_grow, \
			args=( list_code, int( num_stock / num_process ) * num_process, num_stock, \
				df_profit_data, df_stock_basics, queue_process  ) ) )

		for process in list_process:
			process.start()
		for process in list_process:
			process.join()

		while not queue_process.empty():
			ls_get = queue_process.get()
			for ls in ls_get:
				self.df_profit_grow.loc[ self.df_profit_grow.index.size ] = ls

		self.df_profit_grow = self.df_profit_grow.sort_values( by = [ 'profit_grow' ], axis = 0, ascending = False )
		self.df_profit_grow = self.df_profit_grow.set_index( 'code' )
		self.save_data( self.df_profit_grow, self.profit_grow_file )

	# @Utils.func_timer
	# def calc_profit_grow( self, num_thread = 8 ):
		
	# 	def sub_calc_profit_grow( self, list_code, id_start, id_end, df_profit_data, df_stock_basics, lock_df_profit_grow ):

	# 		tmp_df_profit_grow = DataFrame( columns = ( 'code', 'name', 'profit_grow' ) )

	# 		for id in range( id_start, id_end ):
	# 			code = list_code[ id ]
	# 			tmp_df =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
	# 			code = '%06d' % code		
	# 			try:	
	# 				cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
	# 			except:
	# 				# 尚没有k线的股票跳过
	# 				continue
				
	# 			basics = df_stock_basics.loc[ int( code ) ]
	# 			ls_grow_ratio = []

	# 			for i in range( tmp_df.index.size - 4 ):

	# 				if tmp_df.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 5:
	# 				# 统计近4到5年的数据
	# 					if tmp_df.iloc[ i + 4 ][ 'quarter' ] == tmp_df.iloc[ i ][ 'quarter' ] and \
	# 						( tmp_df.iloc[ i + 4 ][ 'year' ] - tmp_df.iloc[ i ][ 'year' ] ) == 1:
							
	# 						if np.isnan( tmp_df.iloc[ i ][ 'net_profits' ] ) or np.isnan( tmp_df.iloc[ i + 4 ][ 'net_profits' ] ):
	# 							continue

	# 						ls_grow_ratio.append( ( tmp_df.iloc[ i + 4 ][ 'net_profits' ] - tmp_df.iloc[ i ][ 'net_profits' ] ) \
	# 												/ ( abs( tmp_df.iloc[ i ][ 'net_profits' ] ) + abs( tmp_df.iloc[ i + 4 ][ 'net_profits' ] ) + 1 ) )
						
	# 			ls_weight = []

	# 			# 指数赋权
	# 			for i in range( len( ls_grow_ratio ) ):
	# 				if ls_grow_ratio[ i ] < 0:
	# 					# 利润增长下降惩罚因子
	# 					eta = 10.0
	# 					ls_weight.append( eta * np.exp( 1 - i * 0.25 ) )
	# 				else:
	# 					ls_weight.append( np.exp( 1 - i * 0.25 ) )
	# 			# for i in range( len( ls_grow_ratio ) ):
	# 			# 	ls_weight.append( 10 - ( 0.1 * i ) )

	# 			ls_weight.reverse()
	# 			arr_weight = np.array( ls_weight ) / sum( np.array( ls_weight ) )

	# 			# profit_grow = 100 * ( 利润增长数组 · 权重数组 ) / 市盈率
	# 			# profit_grow = 100.0 * ( 1.0 + float( np.dot( np.array( ls_grow_ratio ), arr_weight ) ) ) * float( basics[ 'esp' ] ) / cur_price
	# 			profit_grow = 50.0 * ( 1.0 + float( np.dot( np.array( ls_grow_ratio ), arr_weight ) ) )
	# 			profit_grow = float( '{0:.2f}'.format( profit_grow ) )
	# 			name = tmp_df.iloc[ 0 ][ 'name' ]

	# 			tmp_df_profit_grow.loc[ tmp_df_profit_grow.index.size ] = [ code, name, profit_grow ]
	# 			LOG( '{1}	df_size:{0}'.format( tmp_df_profit_grow.index.size, threading.current_thread().getName() ) )

	# 		lock_df_profit_grow.acquire()
	# 		self.df_profit_grow = self.df_profit_grow.append( tmp_df_profit_grow )
	# 		lock_df_profit_grow.release()
		

	# 	df_profit_data = Data().get_profit_data()
	# 	df_profit_data = df_profit_data.set_index( 'code' ).drop_duplicates()

	# 	df_stock_basics = Data().get_stock_basics()
	# 	df_stock_basics = df_stock_basics.set_index( 'code' )

	# 	set_code = set()
	# 	for code in df_profit_data.index:
	# 		set_code.add( code )

	# 	list_code = list( set_code )
	# 	num_stock = len( list_code )

	# 	list_threads = []
	# 	lock_df_profit_grow = threading.Lock()

	# 	for n in range( num_thread ):
	# 		list_threads.append( threading.Thread( target = sub_calc_profit_grow, \
	# 			args=( self, list_code, int( num_stock  / num_thread ) * n, int( num_stock / num_thread ) * ( n + 1 ), \
	# 				df_profit_data, df_stock_basics, lock_df_profit_grow ) ) )
	# 	list_threads.append( threading.Thread( target = sub_calc_profit_grow, \
	# 		args=( self, list_code, int( num_stock / num_thread ) * num_thread, num_stock, \
	# 			df_profit_data, df_stock_basics, lock_df_profit_grow  ) ) )

	# 	for thread in list_threads:
	# 		thread.start()
	# 	for thread in list_threads:
	# 		thread.join()

	# 	self.df_profit_grow = self.df_profit_grow.sort_values( by = [ 'profit_grow' ], axis = 0, ascending = False )
	# 	self.df_profit_grow = self.df_profit_grow.set_index( 'code' )
	# 	LOG( 'self.df_profit_grow.size:{0}'.format( self.df_profit_grow.index.size ) )
	# 	self.save_data( self.df_profit_grow, self.profit_grow_file )

	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.result_path ):
			os.mkdir( self.result_path )
		Utils.save_data( df_data, file_path_name )


if __name__ == '__main__':

	Profit().calc_profit_grow()