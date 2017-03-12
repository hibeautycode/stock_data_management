import sys, os
sys.path.append( '../../stock' )
from utils.utils import Utils, LOG, ERROR
import common.base

class Base( common.base.Base ):

	def __init__( self ):

		self.result_path = '../factor/result/'
		
	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.result_path ):
			os.mkdir( self.result_path )
		Utils.save_data( df_data, file_path_name )

	def multiprocessing_for_single_func( target_func, dict_func_args = {}, num_process = 3 ):

		return common.base.Base.multiprocessing_for_single_func( target_func, dict_func_args, num_process )

	def multiprocessing_for_multi_func( *args, **kwargs ):

		return common.base.Base.multiprocessing_for_multi_func( *args, **kwargs )
