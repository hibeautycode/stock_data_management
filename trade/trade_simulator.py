import sys, os
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
from model.spill_wave import Analyse
from model.basics import Basics
from pandas import DataFrame
from time import sleep
#from multiprocessing import Queue, Process

class Trade_Simulator():

	def __init__( self, model = 'spill_wave' ):

		self.root_file_path = '../trade/position/'
		self.__file_path_trade = '../trade/position/simulate_trade_flow.xlsx'
		self.__file_path_position = '../trade/position/simulate_position.xlsx'
		if model == 'spill_wave':
			self.df_value_stock = Utils.read_data( Analyse().value_stock_file )
		self.remain_money = 200000.0	# 初始投资额
		self.max_buy_each_stock = self.remain_money / 4.0 	# 每股最大买入额
		self.df_trade = DataFrame( columns = ( 'code', 'name', 'type', 'time', 'amount', 'total_earn', 'remain' ) )
		self.df_position = DataFrame( columns = ( 'code', 'name', 'buy_date', 'cost_price', 'sell_price', 'position', 'earn' ) )

	def do_realtime_trade( self ):
		
		df_model_basics = Basics().get_basics().set_index( 'code' )

		if os.path.exists( self.__file_path_trade ):
			self.df_trade = Utils.read_data( self.__file_path_trade )
			self.remain_money = self.df_trade.loc[ self.df_trade.index.size - 1 ][ 'remain' ]
		if os.path.exists( self.__file_path_position ):
			self.df_position = Utils.read_data( self.__file_path_position )
				
		while True:

			cur_time = Utils.cur_time()
			hour = int( cur_time.split( ':' )[ 0 ] )
			minute = int( cur_time.split( ':' )[ 1 ] )
			
			if hour < 9 or ( hour == 9 and minute < 30 ):
				LOG( 'Trade_Simulator:	morning\n{0} hours {1} minutes later market open'\
					.format( int( Utils.now2market_morning_time() / 3600 ), int( Utils.now2market_morning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_morning_time() )
			elif ( hour == 11 and minute >= 30 ) or hour == 12:
				LOG( 'Trade_Simulator:	nooning\n{0} hours {1} minutes later market open'
					.format( int( Utils.now2market_nooning_time() / 3600 ), int( Utils.now2market_nooning_time() % 3600 / 60 ) ) )
				sleep( Utils.now2market_morning_time() )
			elif hour >= 15:
				LOG( 'Trade_Simulator:	market close' )
				break

			# sell
			for index in self.df_position.index:
				# T + 1 交易
				if self.df_position.loc[ index ][ 'buy_date' ] < Utils.cur_date():
					code = '%06d' % self.df_position.loc[ index ][ 'code' ]
					name = self.df_position.loc[ index ][ 'name' ]
					df_realtime_quotes = Data().get_realtime_quotes( code )	
					if float( df_realtime_quotes[ 'price' ] ) >= self.df_position.loc[ index ][ 'sell_price' ]:
						total_earn = '{0:.2f}'.format( ( float( df_realtime_quotes[ 'price' ] ) - float( self.df_position.loc[ index ][ 'cost_price' ] ) ) \
							 * float( self.df_position.loc[ index ][ 'position' ] ) + float( self.df_trade.loc[ self.df_trade.index.size - 1 ][ 'total_earn' ] ) )
						self.df_trade.loc[ self.df_trade.index.size ] = [ code, name, 'sell', ' '.join( [ Utils.cur_date(), Utils.cur_time() ] ), \
							self.df_position.loc[ index ][ 'position' ], total_earn ]
						self.df_position.drop( index )
						continue
				self.df_position[ 'earn' ][ index ] = '{0:.2f}'.format( ( float( df_realtime_quotes[ 'price' ] ) - float( self.df_position.loc[ index ][ 'cost_price' ] ) ) \
					* float( self.df_position.loc[ index ][ 'position' ] ) )

			# buy
			for index in self.df_value_stock.index:
				code = '%06d' % self.df_value_stock.loc[ index ][ 'code' ]
				name = self.df_value_stock.loc[ index ][ 'name' ]
				
				for i in self.df_position.index:
				# 每只在仓股不再买入
					if int( self.df_position.loc[ i ][ 'code' ] ) == int( code ):
						continue

				try:
					df_realtime_quotes = Data().get_realtime_quotes( code )				
					
					if float( df_realtime_quotes[ 'price' ] ) >= ( float( df_value_stock.loc[ index ][ 'buy_price' ] ) * 0.998 ) and \
						float( df_realtime_quotes[ 'price' ] ) <= ( float( df_value_stock.loc[ index ][ 'buy_price' ] ) * 1.002 ) and \
						float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ) >= 0.2:

						try:
							if float( df_model_basics[ 'rank_profit_grow' ][ int( code ) ].split( '/' )[ 0 ] ) / \
								float( df_model_basics[ 'rank_profit_grow' ][ int( code ) ].split( '/' )[ 1 ] ) >= 0.5:
								continue
						except:pass

						buy_hand_num = 0
						if 100 * float( df_realtime_quotes[ 'price' ] ) <= self.remain_money:

							buy_hand_num = int( min( self.total_money * float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ) * 0.5, \
											self.max_buy_each_stock ) / ( float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ) * 100 ) )
							if self.df_trade.index.size >= 1:
								total_earn = self.df_trade.loc[ self.df_trade.index.size - 1 ][ 'total_earn' ]
							else:
								total_earn = 0
							self.remain_money -= buy_hand_num * float( df_realtime_quotes[ 'price' ] )
							self.df_trade.loc[ self.df_trade.index.size ] = [ code, name, 'buy', ' '.join( [ Utils.cur_date(), Utils.cur_time() ] ), \
								buy_hand_num * 100, total_earn, self.remain_money ]
							self.df_position.loc[ self.df_position.index.size ] = [ code, name, Utils.cur_date(), df_realtime_quotes[ 'price' ], \
								df_realtime_quotes[ 'price' ] * ( 0.2 * float( df_value_stock.loc[ index ][ 'expect_earn_rate' ] ) + 1.0 ), \
								buy_hand_num * 100, 0 ]
				except:
					pass

			sleep( 10 )

		self.save_data( self.df_trade, self.__file_path_trade )
		self.save_data( self.df_position, self.__file_path_position )

	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.root_file_path ):
			os.mkdir( self.root_file_path )
		Utils.save_data( df_data, file_path_name )

if __name__ == '__main__':

	Trade_Simulator().do_realtime_trade()