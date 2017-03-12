import numpy as np
import sys, os, datetime
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
import pandas as pd

class Profit():

	def __init__( self ):
		pass

	def calc_profit_grow( self ):

		pd.options.mode.chained_assignment = None  # 不显示warn信息 default='warn'
		df_profit_data = Data().get_profit_data()
		df_profit_data = df_profit_data.set_index( 'code' ).drop_duplicates()

		set_code = set()
		for code in df_profit_data.index:
			set_code.add( code )

		list_code = list( set_code )[0:1]

		for id in range( len( list_code ) ):
			code = list_code[ id ]
			df_tmp =  df_profit_data.loc[ code ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True )
			code = '%06d' % code
			ls_grow_ratio = []
			num_minus_net_profit = 0

			df_year = df_tmp[df_tmp.year == 2014 ]
			df_quar = df_year[df_year.quarter == 1]
			LOG(df_year)
			LOG(df_quar.loc[int(code)]['net_profits'])



		for i in range( 1, df_tmp.index.size - 4 ):

				if df_tmp.iloc[ i ][ 'year' ] >= datetime.datetime.now().year - 3:
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
if __name__ == '__main__':

	Profit().calc_profit_grow()
