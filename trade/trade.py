import sys, os
sys.path.append( '../data' )
sys.path.append( '../utils' )
from data import Data
from utils import Utils, LOG, ERROR, SEND_EMAIL
from time import sleep

class Position():

	def __init__( self ):

		self.__file_path_position = '../trade/data/position.xlsx'

	def notify_realtime_earnings( self ):

		if os.path.exists( self.__file_path_position ):
			df_position = Utils.read_data( self.__file_path_position )
		else:
			ERROR( 'file {0} not exists.'.format( self.__file_path_position ) )
			return

		while True:
		
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

					content_notify += '-{0} {1} cur_price:{2:.2f} buy_price:{3:.2f} sell_price:{4:.2f} position:{5} earn:{6:.2f}\n'\
					.format( code, name, cur_price, buy_price, float( df_position.loc[ index ][ 'sell_price' ] ), position, earn)
					
				except:
					pass
			content_notify += 'total_earn:{0:.2f}'.format( total_earn )
			Utils.send_email( content_notify, 'position notification' )
			LOG( 'notify position.')
			sleep( 600 )

if __name__ == '__main__':

	Position().notify_realtime_earnings()