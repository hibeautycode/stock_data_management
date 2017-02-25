import sys, os
sys.path.append( '../data' )
sys.path.append( '../utils' )
from data import Data
from utils import Utils, LOG, ERROR, SEND_EMAIL
from pandas import DataFrame
from time import sleep

class Position():
# 持仓类

	def __init__( self ):

		self.__file_path_position = '../trade/data/position.xlsx'

	def serve_query_request( self ):

		while True:

			ls_code = Utils.receive_email_query_code()

			LOG( 'ls_code:{0}'.format( ls_code ) )
			
			dict_stock_info = Data().query_stock_info( ls_code )

			for ( code, info ) in dict_stock_info.items():
				Utils.send_email( info, 'stock info ' + code )
			LOG( 'send email stock info' )

			# 每10分钟查一次邮箱是否有查询
			sleep( 600 )


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
			
			if hour < 9 or ( hour == 9 and minute < 30 ) or ( hour == 11 and minute >= 30 ) or hour == 12:
				LOG( 'market not open from notify_realtime_earnings' )
				sleep( 60 )
				continue
			elif hour >= 15:
				LOG( 'market close from notify_realtime_earnings' )
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
			Utils.send_email( content_notify, 'position notification' )
			LOG( 'notify position.')
			LOG( df_position )
			sleep( 600 )

if __name__ == '__main__':

	# Position().notify_realtime_earnings()
	Position().serve_query_request()
	