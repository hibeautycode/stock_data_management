import sys, os
sys.path.append( '../../stock' )
from utils.utils import Utils, LOG, ERROR

class Base( object ):

	def __init__( self ):

		self.result_path = '../factor/result/'
		
	def save_data( self, df_data, file_path_name ):
	
		if not os.path.exists( self.result_path ):
			os.mkdir( self.result_path )
		Utils.save_data( df_data, file_path_name )