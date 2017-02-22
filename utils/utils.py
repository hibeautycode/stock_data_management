import pandas as pd
import sys, os, re, time, datetime
from functools import wraps

from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib

'''--------------- switch definition ---------------'''
SAVE_DATA = 'xls'
SHOW_LOG = True
SHOW_ERROR = True
SEND_EMAIL = True

def LOG( content ):
	if SHOW_LOG:
		print( content )

def ERROR( content ):
	if SHOW_ERROR:
		print( content )

'''--------------- Utils class ---------------'''		
class Utils():
	
	def __init__( self ):
		pass
	
	def parse_date_to_ymd( date ):
		
		pattern = re.compile( '[-_\s]+' )
		list_res = re.split( pattern, date )
		quarter = int( ( int( list_res[1] ) - 1 ) / 3 + 1 )	
		list_res.append( quarter )		
		return list_res
	
	def cur_date():

		return datetime.date.today().strftime( '%Y_%m_%d' )

	def last_date():

		return ( datetime.date.today() - datetime.timedelta( days = 1 ) ).strftime( '%Y_%m_%d' )

	def cur_time():

		return time.strftime('%H:%M:%S',time.localtime( time.time() )  )

	def send_email( content, header = 'stock notification' ):

		def _format_addr( s ):
			name, addr = parseaddr( s )
			return formataddr( ( Header( name, 'utf-8' ).encode(), addr ) )
			
		from_addr = 'qiyubi@126.com'
		password = 'qiyubi1990'
		to_addr = '504571914@qq.com'
		smtp_server = 'smtp.126.com'

		msg = MIMEText( content, 'plain', 'utf-8' )
		msg['From'] = _format_addr( 'abel <%s>' % from_addr )
		msg['To'] = _format_addr( 'qiyubi <%s>' % to_addr )
		msg['Subject'] = Header( header, 'utf-8' ).encode()

		server = smtplib.SMTP( smtp_server, 25 )
		#server.set_debuglevel( 1 )
		server.login( from_addr, password )
		server.sendmail( from_addr, [ to_addr ], msg.as_string() )
		server.quit()
		

	if SAVE_DATA == 'xls':
		
		def read_data( file_name ):
			if os.path.exists( file_name ):
				return pd.read_excel( file_name )
			else:
				return None

			
		def save_data( dataframe, file_name, sheet_name = 'Sheet1' ):
			excel_writer = pd.ExcelWriter( file_name )
			dataframe.to_excel( excel_writer, sheet_name )
			excel_writer.save()
		
		
		def func_timer( function ):
			@wraps(function)
			def function_timer(*args, **kwargs):
				t0 = time.time()
				res = function(*args, **kwargs)
				t1 = time.time()
				print( 'function-{0} running time : {1:.2f} minutes'.format( function.__name__, ( t1-t0 ) / 60 ) )
				return res
			return function_timer

			
		def save_dict_to_xls( dict_to_save, save_name ):
		
			dict_to_save = sorted( dict_to_save.items(), key = operator.itemgetter( 1 ), reverse = True )
			wb = xlwt.Workbook()
			ws = wb.add_sheet('data')
			
			ws.write( 0, 0, 'code' )
			ws.write( 0, 1, 'oscillation_strength' )
			
			row = 1
			for ( key, value ) in dict_to_save:
				ws.write( row, 0, key )
				ws.write( row, 1, str( round( value, 2 ) ) )
				row += 1
			
			wb.save( save_name )
	#else: