import shutil, os

class commit_git():

	def __init__( self ):

		self.__dst_master_dir = '../git_stock/'
		self.__src_master_dir = '../stock/'
		
	def move_file( self, src_dir, dst_dir ):

		if not os.path.exists( dst_dir ):
			os.mkdir( dst_dir )

		for item in os.listdir( src_dir ):

			cur_dst_item = os.path.join( dst_dir, item )
			cur_src_item = os.path.join( src_dir, item )

			if os.path.isdir( cur_src_item ) and os.path.basename( cur_src_item ) in [ 'data', 'factor', 'utils', 'notify', 'query', 'trade' ]:			
				self.move_file( cur_src_item, cur_dst_item )

			elif os.path.isfile( cur_src_item )  and os.path.basename( cur_src_item ).split( '.' )[ -1 ] == 'py' and \
				os.path.basename( cur_src_item ) not in [ 'commit_git.py', 'load_git.py' ]:
				shutil.copyfile( cur_src_item, cur_dst_item )

	def move_all( self ):

		self.move_file( self.__src_master_dir, self.__dst_master_dir )

if __name__ == '__main__':

	commit_git().move_all()
