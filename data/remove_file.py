import os

dir_path = './data/k_line_2017_01_27/'
list_file_name = os.listdir( dir_path )

for file in list_file_name:
	if len( file ) != 11:
		os.remove( dir_path + file )