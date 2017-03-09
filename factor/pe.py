import numpy as np
import sys, os, datetime
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR, SEND_EMAIL
import pandas as pd
from multiprocessing import Queue, Process
from factor.base import Base

class Pe( Base ):

	def __init__( self ):

		Base.__init__( self )
		self.df_average_industry_pe = pd.DataFrame( columns = ( 'industry', 'average_pe' ) )
		self.df_average_concept_pe = pd.DataFrame( columns = ( 'concept', 'average_pe' ) )
		self.df_industry_pe_rank = pd.DataFrame( columns = ( 'code', 'name', 'industry', 'rank_pe' ) )
		self.df_concept_pe_rank = pd.DataFrame( columns = ( 'code', 'name', 'concept_1', 'rank_pe_1', 'concept_2', 'rank_pe_2',\
							'concept_3', 'rank_pe_3', 'concept_4', 'rank_pe_4', 'concept_5', 'rank_pe_5', 'concept_6', 'rank_pe_6',\
							'concept_7', 'rank_pe_7', 'concept_8', 'rank_pe_8', 'concept_9', 'rank_pe_9', 'concept_10', 'rank_pe_10',\
							'concept_11', 'rank_pe_11', 'concept_12', 'rank_pe_12', 'concept_13', 'rank_pe_13', 'concept_14', 'rank_pe_14', \
							'concept_15', 'rank_pe_15', 'concept_16', 'rank_pe_16', 'concept_17', 'rank_pe_17', 'concept_18', 'rank_pe_18',\
							'concept_19', 'rank_pe_19', 'concept_20', 'rank_pe_20' ) )
		self.average_industry_pe_file = self.result_path + 'pe_average_industry.xlsx'
		self.average_concept_pe_file = self.result_path + 'pe_average_concept.xlsx'
		self.industry_pe_rank_file = self.result_path + 'pe_rank_industry.xlsx'
		self.concept_pe_rank_file = self.result_path + 'pe_rank_concept.xlsx'
		
	@Utils.func_timer
	def calc_average_industry_pe( self, df_stock_basics, df_industry_classified ):

		df_stock_basics = df_stock_basics.set_index( 'code' )

		set_industry_class = set( df_industry_classified.c_name )

		for industry in set_industry_class:
			ls_pe = []
			for code in df_industry_classified[ df_industry_classified.c_name == industry ].index:
				try:
					basics = df_stock_basics.loc[ code ]
					code = '{0:06d}'.format( code )
					try:		
						cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
					except:
						try:
							cur_price = float( Data().get_realtime_quotes( code )[ 'price' ] )
						except:
							continue
					if basics[ 'esp' ] == 0:
						continue
					if float( cur_price / basics[ 'esp' ] ) < 0:
						ls_pe.append( 1000000 / abs( float( cur_price / basics[ 'esp' ] ) ) )
					else:
						ls_pe.append( float( cur_price / basics[ 'esp' ] ) )
				except:
					continue
			array_pe = np.array( ls_pe )
			average_pe = float( '{0:.2f}'.format( np.mean( array_pe ) ) )
			self.df_average_industry_pe.loc[ self.df_average_industry_pe.index.size ] = [ industry, average_pe ]

		self.df_average_industry_pe = self.df_average_industry_pe.set_index( 'industry' )
		Base.save_data( self, self.df_average_industry_pe, self.average_industry_pe_file )		
	
	@Utils.func_timer
	def calc_average_concept_pe( self, df_stock_basics, df_concept_classified ):

		df_stock_basics = df_stock_basics.set_index( 'code' )

		set_concept_class = set( df_concept_classified.c_name )

		for concept in set_concept_class:
			ls_pe = []
			for code in df_concept_classified[ df_concept_classified.c_name == concept ].index:
				try:
					basics = df_stock_basics.loc[ code ]
					code = '{0:06d}'.format( code )
					try:		
						cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
					except:
						try:
							cur_price = float( Data().get_realtime_quotes( code )[ 'price' ] )
						except:
							continue
					if basics[ 'esp' ] == 0:
						continue
					if float( cur_price / basics[ 'esp' ] ) < 0:
						ls_pe.append( 1000000.0 / abs( float( cur_price / basics[ 'esp' ] ) ) )
					else:
						ls_pe.append( float( cur_price / basics[ 'esp' ] ) )
				except:
					continue

			array_pe = np.array( ls_pe )
			average_pe = float( '{0:.2f}'.format( np.mean( array_pe ) ) )
			self.df_average_concept_pe.loc[ self.df_average_concept_pe.index.size ] = [ concept, average_pe ]

		self.df_average_concept_pe = self.df_average_concept_pe.set_index( 'concept' )
		Base.save_data( self, self.df_average_concept_pe, self.average_concept_pe_file )

	@Utils.func_timer
	def calc_industry_pe_rank( self, df_stock_basics, df_industry_classified ):

		self.df_industry_pe_rank.code = [ '{0:06d}'.format( code ) for code in df_stock_basics.code ]
		self.df_industry_pe_rank.name = [ '{0}'.format( name ) for name in df_stock_basics.name ]
		self.df_industry_pe_rank = self.df_industry_pe_rank.set_index( 'code' )
		df_stock_basics = df_stock_basics.set_index( 'code' )

		set_industry_class = set( df_industry_classified.c_name )

		# industry pe rank
		for industry in set_industry_class:
			tmp_df_pe = pd.DataFrame( columns = ( 'code', 'pe' ) )
			for code in df_industry_classified[ df_industry_classified.c_name == industry ].index:
				try:
					basics = df_stock_basics.loc[ code ]
					code = '{0:06d}'.format( code )
					try:		
						cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
					except:
						try:
							cur_price = float( Data().get_realtime_quotes( code )[ 'price' ] )
						except:
							continue
					if basics[ 'esp' ] == 0:
						continue
				except:
					continue
				if float( cur_price / basics[ 'esp' ] ) < 0:
					tmp_df_pe.loc[ tmp_df_pe.index.size ] = [ code, 1000000.0 / abs( float( cur_price / basics[ 'esp' ] ) ) ]
				else:
					tmp_df_pe.loc[ tmp_df_pe.index.size ] = [ code, float( cur_price / basics[ 'esp' ] ) ]
				self.df_industry_pe_rank[ 'industry' ][ code ] = industry 

			tmp_df_pe = tmp_df_pe.set_index( 'code' )			
			tmp_df_pe_rank = tmp_df_pe.rank()
			num_code = tmp_df_pe_rank.index.size
			for code in tmp_df_pe_rank.index:
				self.df_industry_pe_rank[ 'rank_pe' ][ code ] = '/'.join( [ str( int( tmp_df_pe_rank.loc[ code ][ 'pe' ] ) ), str( num_code ) ] )
	
		Base.save_data( self, self.df_industry_pe_rank, self.industry_pe_rank_file )

	@Utils.func_timer
	def calc_concept_pe_rank( self, df_stock_basics, df_concept_classified ):

		self.df_concept_pe_rank.code = [ '{0:06d}'.format( code ) for code in df_stock_basics.code ]
		self.df_concept_pe_rank.name = [ '{0}'.format( name ) for name in df_stock_basics.name ]
		self.df_concept_pe_rank = self.df_concept_pe_rank.set_index( 'code' )
		df_stock_basics = df_stock_basics.set_index( 'code' )

		# concept pe rank
		set_concept_class = set( df_concept_classified.c_name )
		
		for concept in set_concept_class:
			tmp_df_pe = pd.DataFrame( columns = ( 'code', 'pe' ) )
			for code in df_concept_classified[ df_concept_classified.c_name == concept ].index:
				
				try:
					basics = df_stock_basics.loc[ code ]
					code = '{0:06d}'.format( code )
					try:		
						cur_price = float( Data().get_k_line_data( code ).iloc[ -1 ][ 'close' ] )
					except:
						try:
							cur_price = float( Data().get_realtime_quotes( code )[ 'price' ] )
						except:
							continue
					if basics[ 'esp' ] == 0:
						continue
				except:
					continue
				if float( cur_price / basics[ 'esp' ] ) < 0:
					tmp_df_pe.loc[ tmp_df_pe.index.size ] = [ code, 1000000.0 / abs( float( cur_price / basics[ 'esp' ] ) ) ]
				else:
					tmp_df_pe.loc[ tmp_df_pe.index.size ] = [ code, float( cur_price / basics[ 'esp' ] ) ]
				id_concept = 1
				name_concept = '_'.join( [ 'concept', str( id_concept ) ] )
				while self.df_concept_pe_rank[ name_concept ][ code ] is not np.nan:
					id_concept += 1
					name_concept = '_'.join( [ 'concept', str( id_concept ) ] )
				self.df_concept_pe_rank[ name_concept ][ code ] = concept
			
			tmp_df_pe = tmp_df_pe.set_index( 'code' )			
			tmp_df_pe_rank = tmp_df_pe.rank()
			num_code = tmp_df_pe_rank.index.size
			for code in tmp_df_pe_rank.index:
				id_rank = 1
				name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
				while self.df_concept_pe_rank[ name_rank ][ code ] is not np.nan:
					id_rank += 1
					name_rank = '_'.join( [ 'rank_pe', str( id_rank ) ] )
				self.df_concept_pe_rank[ name_rank ][ code ] = '/'.join( [ str( int( tmp_df_pe_rank.loc[ code ][ 'pe' ] ) ), str( num_code ) ] )
			
		Base.save_data( self, self.df_concept_pe_rank, self.concept_pe_rank_file )

	def calc_pe( self ):

		df_stock_basics = Data().get_stock_basics()

		df_industry_classified = pd.DataFrame( columns = ( 'code', 'name', 'c_name' ) )
		df_industry_classified.code = df_stock_basics.code
		df_industry_classified.name = df_stock_basics.name
		df_industry_classified.c_name = df_stock_basics.industry
		df_industry_classified = df_industry_classified.set_index( 'code' )

		df_concept_classified = Data().get_concept_classified_data()
		df_concept_classified = df_concept_classified.set_index( 'code' )

		list_process = []
		list_process.append( Process( target = self.calc_average_industry_pe, \
			args=( df_stock_basics, df_industry_classified ) ) )
		list_process.append( Process( target = self.calc_average_concept_pe, \
			args=( df_stock_basics, df_concept_classified ) ) )
		list_process.append( Process( target = self.calc_industry_pe_rank, \
			args=( df_stock_basics, df_industry_classified ) ) )
		list_process.append( Process( target = self.calc_concept_pe_rank, \
			args=( df_stock_basics, df_concept_classified ) ) )

		for process in list_process:
			process.start()
		for process in list_process:
			process.join()

	def get_average_industry_pe( self ):
		return Utils.read_data( self.average_industry_pe_file )

	def get_average_concept_pe( self ):
		return Utils.read_data( self.average_concept_pe_file )

	def get_industry_pe_rank( self ):
		return Utils.read_data( self.industry_pe_rank_file )

	def get_concept_pe_rank( self ):
		return Utils.read_data( self.concept_pe_rank_file )

if __name__ == '__main__':

	Pe().calc_pe()
