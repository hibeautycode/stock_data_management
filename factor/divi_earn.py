import sys
import types
import numpy as np
sys.path.append( '../../stock' )
from data.data import Data
from common.utils import Utils, LOG, ERROR
import pandas as pd
from multiprocessing import Queue
from factor.base import Base

class Divi_Earn(Base):

	def __init__( self ):
		Base.__init__( self )
		self.num_pre_days = 10
		self.max_last_days = 20
		self.df_divi = pd.DataFrame( columns = ( 'code', 'name', 'report_date', 'divi', 'shares', 'earn' ) )
		self.divi_file = self.result_path + 'divi_earn.xlsx'

	def calc_earn( self, df_k_line_data, df_divi_k_line_data ):
		try:
			init_price = df_k_line_data.loc[ df_divi_k_line_data.index[ 0 ] - self.num_pre_days ][ 'low' ]
			max_price = 0
			last_days = min( self.max_last_days, df_k_line_data.index.size - df_divi_k_line_data.index[ 0 ] )
			if last_days < 20:
				return None
			for index in range( df_divi_k_line_data.index[ 0 ], df_divi_k_line_data.index[ 0 ] + last_days ):
				if max_price < df_k_line_data.loc[ index ][ 'high' ]:
					max_price = df_k_line_data.loc[ index ][ 'high' ]
			earn_rate = float( '{0:.2f}'.format( ( max_price - init_price ) / init_price ) )
		except:
			return None
		return earn_rate

	def sub_calc_divi_earn( self, list_code, id_start, id_end, df_divi_data, queue_process ):

		ls_tmp = []
		object_Data = Data()
		for id in range( id_start, id_end ):
			code = list_code[ id ]
			tmp_divi =  df_divi_data.loc[ code ]
			code = '%06d' % code

			# 判断tmp_divi类型
			if type( tmp_divi ) == pd.Series:
				name = tmp_divi[ 'name' ]
			elif type( tmp_divi ) == pd.DataFrame:
				name = tmp_divi.iloc[0][ 'name' ]
			else:
				ERROR( 'tmp_divi type error when calc divi earn' )
				continue
			try:
				# 获取不到k线数据 或 k线数据为空，则跳过
				df_k_line_data = object_Data.get_k_line_data( code )
				if df_k_line_data.index.size == 0 or df_k_line_data.empty:
					continue
			except:
				continue

			earn_rate = 0
			if type( tmp_divi ) == pd.Series:
				date = tmp_divi[ 'report_date' ]
				divi = float( '{0:.2f}'.format( tmp_divi[ 'divi' ] ) )
				shares = tmp_divi[ 'shares' ]
				if divi == 0 and shares == 0:
					continue
				try:
					df_divi_k_line_data = df_k_line_data[ df_k_line_data.date == date ]
				except:
					continue
				else:
					if not df_divi_k_line_data.empty:
						earn_rate = self.calc_earn( df_k_line_data, df_divi_k_line_data )
						if earn_rate is None:
							continue
						ls_tmp.append( [ code, name, date, divi, shares, earn_rate ] )
						LOG([ code, name, date, divi, shares, earn_rate ])
					else:
						continue
			elif type( tmp_divi ) == pd.DataFrame:
				for i in range( tmp_divi.index.size ):
					date = tmp_divi.iloc[ i ][ 'report_date' ]
					divi = float( '{0:.2f}'.format( tmp_divi.iloc[ i ][ 'divi' ] ) )
					shares = tmp_divi.iloc[ i ][ 'shares' ]
					if divi == 0 and shares == 0:
						continue
					try:
						df_divi_k_line_data = df_k_line_data[ df_k_line_data.date == date ]
					except:
						continue
					else:
						if not df_divi_k_line_data.empty:
							earn_rate = self.calc_earn( df_k_line_data, df_divi_k_line_data )
							if earn_rate is None:
								continue
							ls_tmp.append( [ code, name, date, divi, shares, earn_rate ] )
							LOG([ code, name, date, divi, shares, earn_rate ])
						else:
							continue

		queue_process.put_nowait( ls_tmp )
		
	@Utils.func_timer
	def calc_divi_earn( self, num_process = 50 ):
		
		pd.options.mode.chained_assignment = None  # 不显示warn信息 default='warn'
		df_divi_data = Data().get_divi_data()
		df_divi_data = df_divi_data.set_index( 'code' ).drop_duplicates()

		set_code = set()
		for code in df_divi_data.index:
			set_code.add( code )

		list_code = list( set_code )
		queue_process = Queue()

		ls_res = Base.multiprocessing_for_single_func( self.sub_calc_divi_earn, \
			{ 'list_code':list_code, 'df_data':df_divi_data, 'queue':queue_process }, \
			num_process )

		for ls in ls_res:
			self.df_divi.loc[ self.df_divi.index.size ] = ls

		self.save_data( self.df_divi, self.divi_file )

	def get_divi_earn( self ):
		return Utils.read_data( self.divi_file )

if __name__ == '__main__':
	Divi_Earn().calc_divi_earn()
