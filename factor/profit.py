import numpy as np
import sys, os, datetime, threading
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
import pandas as pd
from multiprocessing import Queue, Process

class Profit():

	def __init__( self ):

		self.result_path = '../factor/result/'
		self.df_profit_grow = pd.DataFrame( columns = ( 'code', 'name', 'profit_grow' ) )
		self.profit_grow_file = self.result_path + 'profit_grow.xlsx'

	def sub_calc_profit_grow( self, list_code, id_start, id_end, df_profit_data, df_stock_basics, queue_process ):

		ls_tmp = []

		for id in range( id_start, id_end ):
			code = list_code[ id ]
			df_tmp =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
			code = '%06d' % code		
			try:	
				cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
			except:
				# 尚没有k线的股票跳过
				continue
			
			basics = df_stock_basics.loc[ int( code ) ]
			ls_grow_ratio = []

			for i in range( 1, df_tmp.index.size - 4 ):

				if df_tmp.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 5:
				# 统计近4到5年的数据
					if df_tmp.iloc[ i ][ 'quarter' ] == 1:
						if df_tmp.iloc[ i + 4 ][ 'quarter' ] == df_tmp.iloc[ i ][ 'quarter' ] and \
							( df_tmp.iloc[ i + 4 ][ 'year' ] - df_tmp.iloc[ i ][ 'year' ] ) == 1:
						
							if np.isnan( df_tmp.iloc[ i ][ 'net_profits' ] ) or np.isnan( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ):
								continue

							ls_grow_ratio.append( ( df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i ][ 'net_profits' ] ) \
													/ ( abs( df_tmp.iloc[ i ][ 'net_profits' ] ) + abs( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ) + 0.01 ) )
					else:
						if df_tmp.iloc[ i + 4 ][ 'quarter' ] == df_tmp.iloc[ i ][ 'quarter' ] and \
							df_tmp.iloc[ i + 4 ][ 'year' ] - df_tmp.iloc[ i ][ 'year' ] == 1 and \
							df_tmp.iloc[ i + 3 ][ 'quarter' ] == df_tmp.iloc[ i - 1 ][ 'quarter' ] and \
							df_tmp.iloc[ i + 3 ][ 'year' ] - df_tmp.iloc[ i - 1 ][ 'year' ] == 1 and \
							df_tmp.iloc[ i + 4 ][ 'quarter' ] - df_tmp.iloc[ i + 3 ][ 'quarter' ] == 1:

							if np.isnan( df_tmp.iloc[ i ][ 'net_profits' ] ) or np.isnan( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ):
								continue

							ls_grow_ratio.append( ( df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i + 3 ][ 'net_profits' ] - \
												df_tmp.iloc[ i ][ 'net_profits' ] + df_tmp.iloc[ i - 1 ][ 'net_profits' ] ) \
												/ ( abs( df_tmp.iloc[ i ][ 'net_profits' ] - df_tmp.iloc[ i - 1 ][ 'net_profits' ] ) + \
												abs( df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i + 3 ][ 'net_profits' ] ) + 0.001 ) )
					
			# for i in range( df_tmp.index.size - 4 ):

			# 	if df_tmp.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 5:
			# 	# 统计近4到5年的数据
			# 		if df_tmp.iloc[ i + 4 ][ 'quarter' ] == df_tmp.iloc[ i ][ 'quarter' ] and \
			# 			( df_tmp.iloc[ i + 4 ][ 'year' ] - df_tmp.iloc[ i ][ 'year' ] ) == 1:
						
			# 			if np.isnan( df_tmp.iloc[ i ][ 'net_profits' ] ) or np.isnan( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ):
			# 				continue

			# 			ls_grow_ratio.append( ( df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i ][ 'net_profits' ] ) \
			# 									/ ( abs( df_tmp.iloc[ i ][ 'net_profits' ] ) + abs( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ) + 0.01 ) )
					
			if len( ls_grow_ratio ) == 0:
				continue
			ls_weight = []

			# 指数赋权
			for i in range( len( ls_grow_ratio ) ):
				if ls_grow_ratio[ i ] < 0:
					# 利润增长下降惩罚因子
					eta = 3.0
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
			profit_grow = ( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ) ) * np.log( 2.0 + len( arr_grow_ratio ) ) \
				/ ( 0.2 + float( np.var( arr_grow_ratio ) ) ) # 方差：震荡因子
			# LOG( '{0} {1} {2}'.format( code, profit_grow, float( np.var( arr_grow_ratio ) ) ) )

			name = df_tmp.iloc[ 0 ][ 'name' ]

			ls_tmp.append( [ code, name, profit_grow ] )

		queue_process.put_nowait( ls_tmp )
		
	@Utils.func_timer
	def calc_profit_grow( self, num_process = 30 ):
		
		pd.options.mode.chained_assignment = None  # 不显示warn信息 default='warn'
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

			if len( list_process ) == 3:

				for process in list_process:
					process.start()
				for process in list_process:
					process.join()	

				list_process = []	

				while not queue_process.empty():
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
		# df_profit_grow[ 'profit_grow' ] 归一化处理
		self.df_profit_grow[ 'profit_grow' ] = 100.0 * self.df_profit_grow[ 'profit_grow' ] / self.df_profit_grow[ 'profit_grow' ].max()
		self.df_profit_grow = self.df_profit_grow.sort_values( by = [ 'profit_grow' ], axis = 0, ascending = False )
		self.df_profit_grow = self.df_profit_grow.set_index( 'code' )
		for index in self.df_profit_grow.index:
			self.df_profit_grow[ 'profit_grow' ][ index ] = float( '{0:.2f}'.format( self.df_profit_grow[ 'profit_grow' ][ index ] ) )
		self.save_data( self.df_profit_grow, self.profit_grow_file )

	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.result_path ):
			os.mkdir( self.result_path )
		Utils.save_data( df_data, file_path_name )


if __name__ == '__main__':

	Profit().calc_profit_grow()