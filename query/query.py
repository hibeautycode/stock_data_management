import re
import sys

sys.path.append( '../../stock' )
from data.data import Data
from common.utils import LOG, ERROR
from model.basics import Basics
import pandas as pd
import numpy as np
import tushare as ts

class Query():

	def __init__( self ):
		pass

	def query_stock_info( ls_code, ls_all_stock_data, df_model_basics ):

		[ df_stock_basics, df_quarter_report_data, df_profit_data, df_operation_data, df_growth_data, df_debtpaying_data, \
			df_cashflow_data, df_divi_data, df_forcast_quarter_report_data, df_restrict_stock_data, df_concept_classified ] = ls_all_stock_data

		space = lambda x : ' ' * x # 方便区分不同季度数据	
		pd.options.mode.chained_assignment = None  # 不显示warn信息 default='warn'
		dict_stock_info = {}

		for code in ls_code:

			try:
				basics = df_stock_basics.loc[ int( code ) ]
				content = '\n{0}  {1}\n'.format( code, basics[ 'name' ] )	
				try:		
					cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
				except:
					cur_price = float( Data().get_realtime_quotes( code )[ 'price' ] )

				content += '\nbasics:\n上市日期：{0}\n所属行业：{1}\t行业市盈率排名：{6}\n地区：{2}\n市盈率(动态)：{3}\n市盈率(静态)：{4:.2f}\n市净率：{5}\n'\
					.format( basics[ 'timeToMarket' ], basics[ 'industry' ], basics[ 'area' ], basics[ 'pe' ], \
							cur_price / float( basics[ 'esp' ] ), float( basics[ 'pb' ] ), df_model_basics[ 'rank_pe' ][ int( code ) ] )
				content += '每股公积金：{0}\n每股未分配利润：{1}\n'\
					.format( basics[ 'reservedPerShare' ], basics[ 'perundp' ] )
				content += '总市值：{0:.2f} 亿元\n流动市值：{1:.2f} 亿元\n'\
					.format( cur_price * float( basics[ 'totals' ] ), cur_price * float( basics[ 'outstanding' ] ) )
				content += '总资产：{0:.2f} 亿元\n固定资产：{1:.2f} 亿元\n流动资产：{2:.2f} 亿元\n'\
					.format( float( basics[ 'totalAssets' ] ) / 10000, float( basics[ 'fixedAssets' ] ) / 10000, \
							float( basics[ 'liquidAssets' ] ) / 10000 )
			except: 
				content = '\n{0}\n'.format( code )

			try:
				content += '\nconcept:\n'

				id_concept = 1;	id_rank = 1
				name_concept = '_'.join( [ 'concept', str( id_concept ) ] );	name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
				
				while df_model_basics[ name_concept ][ int( code ) ] is not np.nan:
					content += '{0}  市盈率排名:{1}\n'.format( df_model_basics[ name_concept ][ int( code ) ], \
						df_model_basics[ name_rank ][ int( code ) ] )
					id_concept += 1;	id_rank += 1
					if id_concept > 20:
						break
					name_concept = '_'.join( [ 'concept', str( id_concept ) ] );	name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
				
				content += '\n'
			except: pass

			try: 
				profit = df_profit_data.loc[ int( code ) ].sort_values( by = [ 'year', 'quarter' ], axis = 0, ascending = True ).drop_duplicates()
				content += '\nprofit:\n排名：{0}\n年份   季度  净资产收益率  净利润（百万）  每股收益（元）每股主营业务收入(元)\n'\
					.format( df_model_basics[ 'rank_profit_grow' ][ int( code ) ] )
				for id in range( profit.index.size ):
					content += '{5}{0}  {1}  {2:-10.2f}  {3:-12.2f}  {4:-15.2f}  {6:-20.2f}\n'.format( profit.iloc[ id ][ 'year' ], profit.iloc[ id ][ 'quarter' ], \
							profit.iloc[ id ][ 'roe' ], profit.iloc[ id ][ 'net_profits' ], profit.iloc[ id ][ 'eps' ], \
							space( int( profit.iloc[ id ][ 'quarter' ] ) - 1 ), profit.iloc[ id ][ 'bips' ] )
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
				divi = df_divi_data.loc[ int( code ) ]
				content += '\ndivision:\n年份    公布日期  分红金额(每10股)  转增股数(每10股)\n'
				
				if type( divi ) == pd.Series:
					divi = divi.to_frame().T
					
				if type( divi ) == pd.DataFrame:
					divi = divi.sort_values( by = [ 'year', 'report_date' ], axis = 0, ascending = True )
					for id in range( divi.index.size ):
						content += '{0}  {1}  {2:-12d}  {3:-16d}\n'.format( divi.iloc[ id ][ 'year' ], divi.iloc[ id ][ 'report_date' ], int( divi.iloc[ id ][ 'divi' ] ), \
						int( divi.iloc[ id ][ 'shares' ] ) )
				else:
					ERROR( 'divi type error.' )
			except: pass

			try:
				forcast_quarter_data = df_forcast_quarter_report_data.loc[ int( code ) ]
				content += '\nforcast quarter report:\n发布日期    业绩变动类型  上年同期每股收益  业绩变动范围\n'

				if type( forcast_quarter_data ) == pd.Series:
					forcast_quarter_data = forcast_quarter_data.to_frame().T
				
				if type( forcast_quarter_data ) == pd.DataFrame:
					forcast_quarter_data = forcast_quarter_data.sort_values( by = 'report_date', axis = 0, ascending = True )
					for id in range( forcast_quarter_data.index.size ):
						content += '{0}  {1:>8s}  {2:-14.2f}  {3:>12s}\n'.format( forcast_quarter_data.iloc[ id ][ 'report_date' ], \
							forcast_quarter_data.iloc[ id ][ 'type' ], float( forcast_quarter_data.iloc[ id ][ 'pre_eps' ] ), \
							forcast_quarter_data.iloc[ id ][ 'range' ] )
				else:
					ERROR( 'forcast_quarter_data type error.' )
			except: pass

			try:
				restrict = df_restrict_stock_data.loc[ int( code ) ]
				content += '\nrestrict:\n解禁日期    解禁数量（万股）  占总盘比率\n'

				if type( restrict ) == pd.Series:
					restrict = restrict.to_frame().T
					
				if type( restrict ) == pd.DataFrame:
					restrict = restrict.sort_values( by = 'date', axis = 0, ascending = True )
					for id in range( restrict.index.size ):
						content += '{0}  {1:-12.2f}  {2:-10.2f}\n'.format( restrict.iloc[ id ][ 'date' ], \
								float( restrict.iloc[ id ][ 'count' ] ), float( restrict.iloc[ id ][ 'ratio' ] ) )
				else:
					ERROR( 'restrict type error.' )
			except: pass

			try:
				df_news = ts.get_notices( code )
				content += '\nnotice:\n'
				for index in range( 0, 10 ): # df_news.index:
					content += '{3}、{0}\t{1}\tdate:{2}\n'.format( df_news[ 'title' ][ index ], \
						df_news[ 'type' ][ index ], df_news[ 'date' ][ index ], index + 1 )
					if index < 3:
						content += ts.notice_content( df_news[ 'url' ][ index ] )
						content += '\n\n'
					content += '\n'
			except: pass

			LOG( content )
			dict_stock_info[ code ] = content
		return dict_stock_info

if __name__ == '__main__':

	df_model_basics = Basics().get_basics().set_index( 'code' )
	ls_all_stock_data = Data().get_all_stock_data()
	df_latest_news = ts.get_latest_news( top = 1000 )

	while True:
		content = ''
		str_input = input( 'input stock code or \'news\':\n' )
		try:
			int( str_input )
			pattern = re.compile( '[●┊\-■：∶%；！？;&.,:?!．‘’“”"\'、，。><（()）\[\]\{\}【】―《》『』/／・…_——\s]+' )
			ls_code = re.split( pattern, str_input.strip() )
			Query.query_stock_info( ls_code, ls_all_stock_data, df_model_basics )
		except:
			if len( str_input.split() ) == 1:
				for index in range( 0, 20 ):
					content += '{0}、{1}\t{2}\n'.format( index, df_latest_news[ 'title' ][ index ], df_latest_news[ 'time' ][ index ] )
			elif len( str_input.split() ) == 2:
				content = '{0}'.format( ts.latest_content( df_latest_news[ 'url' ][ int( str_input.split()[ 1 ] ) ] ) )
			elif len( str_input.split() ) == 3:
				for index in range( int( str_input.split()[ 1 ] ), int( str_input.split()[ 2 ] ) ):
					content += '{0}、{1}\t{2}\n'.format( index, df_latest_news[ 'title' ][ index ], df_latest_news[ 'time' ][ index ] )
			LOG( content )
