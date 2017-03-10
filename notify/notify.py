import sys, os
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
from model.spill_wave import Analyse
from model.basics import Basics
from factor.profit import Profit
from factor.pe import Pe
from query.query import Query
from pandas import DataFrame
from time import sleep
from multiprocessing import Queue, Process
import operator as op
import numpy as np

class Notify():

	def __init__( self ):

		self.__file_path_position = '../notify/position/position.xlsx'

	def serve_query_request( self ):

		ls_code_queried = []
		df_model_basics = Basics().get_basics().set_index( 'code' )
		ls_all_stock_data = Data().get_all_stock_data()

		if SEND_EMAIL:

			while True:

				ls_code = Utils.receive_email_query_code()

				# LOG( 'query code:{0}'.format( ls_code ) )

				if not len( ls_code ) or op.eq( ls_code_queried, ls_code ):
				# 每3分钟查一次邮箱是否有查询,没有或查询代码没有更新，则继续等待
					sleep( 180 )
					continue

				ls_code_queried = ls_code
				
				dict_stock_info = Query.query_stock_info( ls_code, ls_all_stock_data, df_model_basics )

				for ( code, info ) in dict_stock_info.items():
					Utils.send_email( info, 'stock info ' + code )
					# LOG( 'send stock info {0}'.format( code ) )

				# 每3分钟查一次邮箱是否有查询
				sleep( 180 )


	def notify_realtime_earnings( self ):
	# 实时持仓盈亏检测通知

		while True:

			df_position = DataFrame()
			if os.path.exists( self.__file_path_position ):
				try:
					df_position = Utils.read_data( self.__file_path_position )
				except:
				# 可能在修改文件，等一分钟
					sleep( 60 )
					continue
			else:
				ERROR( 'file {0} not exists.'.format( self.__file_path_position ) )
				return
		
			cur_time = Utils.cur_time()
			hour = int( cur_time.split( ':' )[ 0 ] )
			minute = int( cur_time.split( ':' )[ 1 ] )
			
			if hour < 9 or ( hour == 9 and minute < 30 ):
				LOG( 'notify_realtime_earnings:	morning\n{0} hours {1} minutes later market open'\
					.format( int( Utils.now2market_morning_time() / 3600 ), int( Utils.now2market_morning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_morning_time() )
			elif ( hour == 11 and minute >= 30 ) or hour == 12:
				LOG( 'notify_realtime_earnings:	nooning\n{0} hours {1} minutes later market open'
					.format( int( Utils.now2market_nooning_time() / 3600 ), int( Utils.now2market_nooning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_nooning_time() )
			elif hour >= 15:
				LOG( 'notify_realtime_earnings:	market close' )
				break
			
			content_notify = ''
			content_notify += '{0}\n'.format( cur_time )
			total_earn = 0

			for index in df_position.index:
				code = '%06d' % df_position.loc[ index ][ 'code' ]
				name = df_position.loc[ index ][ 'name' ]
				try:
					df_realtime_quotes = Data().get_realtime_quotes( code )
					
					buy_price = float( df_position.loc[ index ][ 'buy_price' ] )
					cur_price = float( df_realtime_quotes[ 'price' ] )
					position = df_position.loc[ index ][ 'position' ]
					earn = ( cur_price - buy_price ) * position
					total_earn += earn

					content_notify += '-{0} {1} cur:{2:.2f} cost:{3:.2f} sell:{4:.2f} position:{5} earn:{6:.2f}\n'\
						.format( code, name, cur_price, buy_price, float( df_position.loc[ index ][ 'sell_price' ] ), position, earn)
					
				except:
					pass
			content_notify += 'total_earn:{0:.2f}'.format( total_earn )
			if SEND_EMAIL:
				Utils.send_email( content_notify, 'position notification' )
				sleep( 60 * 5 )
			else:
				LOG( content_notify )
				sleep( 60 )


	def notify_investment_opportunity( self ):
		
		df_spill_wave_stock = Utils.read_data( Analyse().spill_wave_stock_file )
		df_model_basics = Basics().get_basics().set_index( 'code' )
		
		while True:
		
			cur_time = Utils.cur_time()
			hour = int( cur_time.split( ':' )[ 0 ] )
			minute = int( cur_time.split( ':' )[ 1 ] )
			
			if hour < 9 or ( hour == 9 and minute < 30 ):
				LOG( 'notify_investment_opportunity:	morning\n{0} hours {1} minutes later market open'\
					.format( int( Utils.now2market_morning_time() / 3600 ), int( Utils.now2market_morning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_morning_time() )
			elif ( hour == 11 and minute >= 30 ) or hour == 12:
				LOG( 'notify_investment_opportunity:	nooning\n{0} hours {1} minutes later market open'
					.format( int( Utils.now2market_nooning_time() / 3600 ), int( Utils.now2market_nooning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_nooning_time() )
			elif hour >= 15:
				LOG( 'notify_investment_opportunity:	market close' )
				break

			content_notify = ''
			content_notify += '{0}\n'.format( cur_time )
			for index in df_spill_wave_stock.index:
				code = '%06d' % df_spill_wave_stock.loc[ index ][ 'code' ]
				name = df_spill_wave_stock.loc[ index ][ 'name' ]
				try:
					df_realtime_quotes = Data().get_realtime_quotes( code )				
				
					if float( df_realtime_quotes[ 'price' ] ) >= ( float( df_spill_wave_stock.loc[ index ][ 'buy_price' ] ) * 0.99 ) :
						content_notify += '-{0}  {1}  cur price:{2:.2f}  buy price:{3:.2f}  sell price:{4:.2f}  expect earn:{5:.2f}\n'\
							.format( code, name, float( df_realtime_quotes[ 'price' ] ), \
									float( df_spill_wave_stock.loc[ index ][ 'buy_price' ] ), \
									float( df_spill_wave_stock.loc[ index ][ 'sell_price' ] ), \
									float( df_spill_wave_stock.loc[ index ][ 'expect_earn_rate' ] ), \
									float( df_spill_wave_stock.loc[ index ][ 'min_earn_rate' ] ) )	
						content_notify += '\tprofit rank:{0}\n  \tindustry:{1}  pe rank:{2}\n'.format( df_model_basics[ 'rank_profit_grow' ][ int( code ) ],\
							df_model_basics[ 'industry' ][ int( code ) ], df_model_basics[ 'rank_pe' ][ int( code ) ] )

						id_concept = 1;	id_rank = 1
						name_concept = '_'.join( [ 'concept', str( id_concept ) ] );	name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
						
						while df_model_basics[ name_concept ][ int( code ) ] is not np.nan:
							content_notify += '\tconcept:{0}  pe rank:{1}\n'.format( df_model_basics[ name_concept ][ int( code ) ], \
								df_model_basics[ name_rank ][ int( code ) ] )
							id_concept += 1;	id_rank += 1
							if id_concept > 20:
								break
							name_concept = '_'.join( [ 'concept', str( id_concept ) ] );	name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
						
						content_notify += '\n'
				except:
					pass
			if SEND_EMAIL:
			# 如果发送邮件，10分钟发一次
				Utils.send_email( content_notify, 'opportunity notification' )
				sleep( 10 * 60 )
			else:
				LOG( '*********************************' )			
				LOG( content_notify )
				LOG( '*********************************' )										
				sleep( 120 )

if __name__ == '__main__':

	if int( Utils.cur_time().split( ':' )[ 0 ] ) < 18:
	# 18点之前，采用前一个交易日的分析结果
		analyse_class = Analyse()

		list_process = []
		list_process.append( Process( target = Notify().notify_investment_opportunity ) )
		list_process.append( Process( target = Notify().notify_realtime_earnings ) )
		list_process.append( Process( target = Notify().serve_query_request ) )
		
		for process in list_process:
			process.start()
		for process in list_process:
			process.join()
	else:
	# 当日18点之后方可更新数据	
		file_date = Utils.cur_date()
		Data( file_date ).update_all()
		analyse_class = Analyse( file_date )
		# analyse_class.statistics()
		
		list_process = []
		list_process.append( Process( target = analyse_class.find_spill_wave_stock ) )
		list_process.append( Process( target = Profit().calc_profit_grow ) )
		list_process.append( Process( target = Pe().calc_pe ) )

		for process in list_process:
			process.start()
		for process in list_process:
			process.join()

		Basics().create_basics_table()