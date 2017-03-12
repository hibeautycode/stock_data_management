import numpy as np
import sys, os, datetime
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
import pandas as pd
from multiprocessing import Queue, Process
from factor.base import Base

class Profit( Base ):

	def __init__( self ):
		Base.__init__( self )
		self.df_profit_grow = pd.DataFrame( columns = ( 'code', 'name', 'profit_grow', 'rank' ) )
		self.profit_grow_file = self.result_path + 'profit_grow.xlsx'

	def sub_calc_profit_grow( self, list_code, id_start, id_end, df_profit_data, queue_process ):

		ls_tmp = []

		for id in range( id_start, id_end ):
			code = list_code[ id ]
			df_tmp =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
			code = '%06d' % code				
			ls_grow_ratio = []
			ls_year_grow_ratio = [ [], [], [], [] ]
			num_minus_net_profit = 0

			for year in range( datetime.datetime.now().year - 5, datetime.datetime.now().year ):
			# 统计近3到4年的数据
				for quarter in range( 1, 5 ):
					try:
						ls_df_year = [ df_tmp[ df_tmp.year == year ], df_tmp[ df_tmp.year == year + 1 ] ]
						ls_quarter_profit = [ ls_df_year[ 0 ][ ls_df_year[ 0 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ], \
											  ls_df_year[ 1 ][ ls_df_year[ 1 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ] ]
						# 按照各个季度利润计算
						# if quarter == 1:
						# 	ls_quarter_profit = [ ls_df_year[ 0 ][ ls_df_year[ 0 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ], \
						# 					  ls_df_year[ 1 ][ ls_df_year[ 1 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ] ]
						# else:
						# 	ls_quarter_profit = [ ls_df_year[ 0 ][ ls_df_year[ 0 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ] \
						# 		- ls_df_year[ 0 ][ ls_df_year[ 0 ].quarter == quarter - 1 ].loc[ int( code ) ][ 'net_profits' ], \
						# 		ls_df_year[ 1 ][ ls_df_year[ 1 ].quarter == quarter ].loc[ int( code ) ][ 'net_profits' ]\
						# 		- ls_df_year[ 1 ][ ls_df_year[ 1 ].quarter == quarter - 1 ].loc[ int( code ) ][ 'net_profits' ] ]
						if np.isnan( ls_quarter_profit[ 1 ] ) or np.isnan( ls_quarter_profit[ 0 ] ):
							continue
						ls_grow_ratio.append( ( ls_quarter_profit[ 1 ] - ls_quarter_profit[ 0 ] ) \
								/ ( abs( ls_quarter_profit[ 1 ] ) + abs( ls_quarter_profit[ 0 ] ) ) )
						if ls_quarter_profit[ 1 ] < 0:
							num_minus_net_profit += 1
						if ls_quarter_profit[ 0 ] < 0:
							num_minus_net_profit += 1

						ls_year_grow_ratio[ quarter - 1 ].append( abs( ls_quarter_profit[ 1 ] - ls_quarter_profit[ 0 ] ) \
							/ min( abs( ls_quarter_profit[ 1 ] ), abs( ls_quarter_profit[ 0 ] ) ) )
					except:
						continue

			if len( ls_grow_ratio ) <= 0:
				continue
			# 突变因子
			factor_mutation = 1.0
			if len( ls_grow_ratio ) < 4:
			# 刚上市股票给予一定平衡
				factor_mutation = 1.2
			for ls_quarter in ls_year_grow_ratio:
			# 削减部分合并报表和募集的股票影响分析
				for r in ls_quarter:
					if r >= 8.0:
						factor_mutation += r / 8.0
			ls_weight = []
			# 指数赋权
			for i in range( len( ls_grow_ratio ) ):
				if ls_grow_ratio[ i ] < 0:
					# 利润增长下降惩罚因子
					eta = 3.0
					ls_weight.append( eta * np.exp( 1.5 - i * ( i * 0.05 + 0.25 ) ) )
				else:
					ls_weight.append( np.exp( 1.5 - i * ( i * 0.05 + 0.25 ) ) )
			
			ls_weight.reverse()
			arr_weight = np.array( ls_weight ) / sum( np.array( ls_weight ) )
			arr_grow_ratio = np.array( ls_grow_ratio )

			# np.log( 5.5 + len( arr_grow_ratio ) ) 季度数平衡因子;		float( 0.2 + np.var( arr_grow_ratio ) ) 方差-震荡因子;
			# ( 1.0 - num_minus_net_profit / 30 ) 将亏损公司排名靠后	
			# profit_grow = ( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ) ) * np.log( 5.5 + len( arr_grow_ratio ) ) \
			# 	/ np.exp( 2.0 * float( np.var( arr_grow_ratio ) ) ) * ( 1.0 - num_minus_net_profit / 20 )

			profit_grow = ( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ) ) \
				/ factor_mutation / float( 0.3 + np.var( arr_grow_ratio ) ) * ( 1.0 - num_minus_net_profit / 20 )

			# if code in [ '002196', '300376', '002460', '002230', '002394' ]:
			# 	LOG( '{3} grow:{0}, factor_mutation:{1}, minus:{2}, var:{4}, ls_grow_ratio:{5}\n'.format( 1.0 + float( np.dot( arr_grow_ratio, arr_weight ) ), \
			# 		factor_mutation, ( 1.0 - num_minus_net_profit / 30 ), code, float( 0.3 + np.var( arr_grow_ratio ) ), \
			# 		ls_grow_ratio ) )

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
			try:
				self.df_profit_grow[ 'profit_grow' ][ index ] = float( '{0:.2f}'.format( self.df_profit_grow[ 'profit_grow' ][ index ] ) )
				self.df_profit_grow[ 'rank' ][ index ] = '/'.join( [ str( int( tmp_profit_grow_rank[ 'profit_grow' ][ index ] ) ),\
														str( self.df_profit_grow.index.size ) ] )
			except: pass
		Base.save_data( self, self.df_profit_grow, self.profit_grow_file )

	def get_profit_grow( self ):
		return Utils.read_data( self.profit_grow_file )

if __name__ == '__main__':

	Profit().calc_profit_grow()
