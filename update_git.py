import shutil, os

class update_git():

	def __init__( self ):

		self.__dst_master_dir = '../git_stock/'
		self.__src_master_dir = '../stock/'
		self.__ls_sec_level_dir = [ 'data/', 'utils/', 'trade/', 'factor/' ]
		self.__ls_file = [ 'data.py', 'utils.py', 'trade.py', 'oscillation.py' ]

	def move_file( self ):

		if not os.path.exists( self.__dst_master_dir ):
			os.mkdir( self.__dst_master_dir )

		for dir in self.__ls_sec_level_dir:
			cur_dir = self.__dst_master_dir + dir
			if not os.path.exists( cur_dir ):
				os.mkdir( cur_dir )

		for id in range( len( self.__ls_file ) ):
			shutil.copyfile( self.__src_master_dir + self.__ls_sec_level_dir[ id ] + self.__ls_file[ id ], \
				self.__dst_master_dir + self.__ls_sec_level_dir[ id ] + self.__ls_file[ id ] )

if __name__ == '__main__':

	update_git().move_file()
