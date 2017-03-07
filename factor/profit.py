import numpy as np
import sys, os, datetime
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
import pandas as pd
from multiprocessing import Queue, Process

class Profit():

	def __init__( self ):

		self.result_path = '../factor/result/'
		self.df_profit_grow = pd.DataFrame( columns = ( 'code', 'name', 'profit_grow', 'rank' ) )
		self.profit_grow_file = self.result_path + 'profit_grow.xlsx'

	def sub_calc_profit_grow( self, list_code, id_start, id_end, df_profit_data, queue_process ):

		ls_tmp = []

		for id in range( id_start, id_end ):
			code = list_code[ id ]
			df_tmp =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
			code = '%06d' % code				
			ls_grow_ratio = []
			num_minus_net_profit = 0

			for i in range( 1, df_tmp.index.size - 4 ):

				if df_tmp.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 4:
				# 统计近3到4年的数据
					if df_tmp.iloc[ i ][ 'quarter' ] == 1:
						if df_tmp.iloc[ i + 4 ][ 'quarter' ] == df_tmp.iloc[ i ][ 'quarter' ] and \
							( df_tmp.iloc[ i + 4 ][ 'year' ] - df_tmp.iloc[ i ][ 'year' ] ) == 1:
						
							if np.isnan( df_tmp.iloc[ i ][ 'net_profits' ] ) or np.isnan( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ):
								continue

							ls_grow_ratio.append( ( df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i ][ 'net_profits' ] ) \
													/ ( abs( df_tmp.iloc[ i ][ 'net_profits' ] ) + abs( df_tmp.iloc[ i + 4 ][ 'net_profits' ] ) + 0.01 ) )
							if df_tmp.iloc[ i + 4 ][ 'net_profits' ] < 0:
								num_minus_net_profit += 1
							if df_tmp.iloc[ i ][ 'net_profits' ] < 0:
								num_minus_net_profit += 1
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
							if df_tmp.iloc[ i + 4 ][ 'net_profits' ] - df_tmp.iloc[ i + 3 ][ 'net_profits' ] < 0:
								num_minus_net_profit += 1
							if df_tmp.iloc[ i ][ 'net_profits' ] - df_tmp.iloc[ i - 1 ][ 'net_profits' ] < 0:
								num_minus_net_profit += 1
					
			if len( ls_grow_ratio ) <= 0:
				continue
			ls_weight = []

			# 指数赋权
			for i in range( len( ls_grow_ratio ) ):
				if ls_grow_ratio[ i ] < 0:
					# 利润增长下降惩罚因子
					eta = 3.0
					ls_weight.append( eta * np.exp( 1.5 - i * ( i * 0.02 + 0.25 ) ) )
				else:
					ls_weight.append( np.exp( 1.5 - i * ( i * 0.02 + 0.25 ) ) )
			
			ls_weight.reverse()
			arr_weight = np.array( ls_weight ) / sum( np.array( ls_weight ) )
			arr_grow_ratio = np.array( ls_grow_ratio )

			# np.log( 3.0 + len( arr_grow_ratio ) ) 季度数平衡因子;		float( np.var( arr_grow_ratio ) ) 方差-震荡因子;
			# ( 1.0 - num_minus_net_profit / 30 ) 将亏损公司排名靠后	
			profit_grow = ( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ) ) * np.log( 5.5 + len( arr_grow_ratio ) ) \
				/ ( 0.2 + float( np.var( arr_grow_ratio ) ) ) * ( 1.0 - num_minus_net_profit / 30 )

			# LOG( '{0} {1} {2}'.format( code, profit_grow, ls_weight ) )
			name = df_tmp.iloc[ 0 ][ 'name' ]
			ls_tmp.append( [ code, name, profit_grow, np.nan ] )

		queue_process.put_nowait( ls_tmp )
		
	@Utils.func_timer
	def calc_profit_grow( self, num_process = 30 ):
		
		pd.options.mode.chained_assignment = None  # 不显示warn信息 default='warn'
		df_profit_data = Data().get_profit_data()
		df_profit_data = df_profit_data.set_index( 'code' ).drop_duplicates()

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
					df_profit_data, queue_process ) ) )

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
				df_profit_data, queue_process  ) ) )

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
		
		tmp_profit_grow_rank = self.df_profit_grow.rank( ascending = False )
		for index in self.df_profit_grow.index:
			self.df_profit_grow[ 'profit_grow' ][ index ] = float( '{0:.2f}'.format( self.df_profit_grow[ 'profit_grow' ][ index ] ) )
			self.df_profit_grow[ 'rank' ][ index ] = '/'.join( [ str( int( tmp_profit_grow_rank[ 'profit_grow' ][ index ] ) ),\
													str( self.df_profit_grow.index.size ) ] )
		self.save_data( self.df_profit_grow, self.profit_grow_file )

	def get_profit_grow( self ):
		return Utils.read_data( self.profit_grow_file )

	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.result_path ):
			os.mkdir( self.result_path )
		Utils.save_data( df_data, file_path_name )


if __name__ == '__main__':

	Profit().calc_profit_grow()