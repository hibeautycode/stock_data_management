import sys
sys.path.append( '../../stock' )
from multiprocessing import Process


class Base( object ):

	def __init__( self ):
		pass

	def multiprocessing_for_single_func( target_func, dict_func_args = {}, num_process = 10 ):

		list_process = []
		ls_res = []
		func_args = tuple()

		if 'list_code' in dict_func_args:
			ls_code = dict_func_args['list_code']
			num_code = len( ls_code )

		for n in range( num_process ):
			if 'list_code' in dict_func_args:
				func_args = ( ls_code, int( num_code  / num_process ) * n, int( num_code / num_process ) * ( n + 1 ), \
					dict_func_args[ 'df_profit_data' ], dict_func_args[ 'queue' ] )
			list_process.append( Process( target = target_func, args = func_args ) )

			if len( list_process ) == 3:

				for process in list_process:
					process.start()
				for process in list_process:
					process.join()

				list_process = []

				if 'queue' in dict_func_args:
					while not dict_func_args[ 'queue' ].empty():
						ls_get = dict_func_args[ 'queue' ].get()
						for ls in ls_get:
							ls_res.append( ls )

		if 'list_code' in dict_func_args:
			func_args = ( ls_code, int( num_code  / num_process ) * num_process, num_code, \
				dict_func_args[ 'df_profit_data' ], dict_func_args[ 'queue' ] )
		list_process.append( Process( target = target_func, args = func_args ) )

		for process in list_process:
			process.start()
		for process in list_process:
			process.join()

		if 'queue' in dict_func_args:
			while not dict_func_args[ 'queue' ].empty():
				ls_get = dict_func_args[ 'queue' ].get()
				for ls in ls_get:
					ls_res.append( ls )

		return ls_res

	def multiprocessing_for_multi_func( ls_target_func, ls_args = None ):

		list_process = []
		num_process = len( ls_target_func )

		for i in range( num_process ):
			if ls_args is None:
				list_process.append( Process( target = ls_target_func[ i ] ) )
			else:
				list_process.append( Process( target = ls_target_func[ i ], args = ls_args[ i ] ) )

		for process in list_process:
			process.start()
		for process in list_process:
			process.join()
