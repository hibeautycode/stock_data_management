import sys, os
sys.path.append( '../../stock' )
from data.data import Data
from utils.utils import Utils, LOG, ERROR
import tushare as ts
import pandas as pd

class Oscillation():

	def __init__( self, mode = 'fixed_interval', head_to_start_or_end = 30, end_to_tail = 0, threshold_market_days = 40 ):
	# mode = 'elastic_interval' # fixed prefix and suffix
	# mode = 'fixed_interval'
	# note: k_line数据总区间划分为[ start head end tail ] 震荡计算区间为[ head : end ]
		self.mode = mode
		
		if self.mode == 'fixed_interval':
			self.calc_head_to_start_or_end = head_to_start_or_end		# 股票震荡计算开始日距end天数(head_to_end)，在计算具体股票时赋值，用股票上市总天数减去当前值
		elif self.mode == 'elastic_interval':
			self.calc_head_to_start_or_end = head_to_start_or_end		# 股票震荡计算开始日距start天数(head_to_start)
		else:
			ERROR( 'init mode error' )
		
		self.calc_end_to_tail = end_to_tail		# 股票震荡计算end距tail天数
		self.threshold_min_market_days = threshold_market_days + head_to_start_or_end + end_to_tail	# 股票上市天数阈值			
		self.threshold_oscillation_aptitude = 0.05	 # 震荡幅度阈值
		self.path_result = './result/'
	
	def calc_oscillation_strength( self, code, method = '1' ):
		# method = 1:calc_oscillation_strength_by_meet_aptitude_days
		# method = 2:calc_oscillation_strength_by_aptitude_expectation
	
		k_data = Data().get_k_line_data( code )
		num_market_days = k_data.index.size
		
		if num_market_days < self.threshold_min_market_days:	
			return 0
		
		if self.mode == 'fixed_interval':
			tmp_start_day = num_market_days - self.calc_head_to_start_or_end - self.calc_end_to_tail
		else:
			tmp_start_day = self.calc_head_to_start_or_end
		
		series_oscillation = ( ( k_data.get('high') - k_data.get('low') ) / k_data.get('close') )[ tmp_start_day : ( num_market_days - self.calc_end_to_tail ) ]
		
		count_oscillation = 0
		
		if method == '1':
			for k in range( tmp_start_day, num_market_days - self.calc_end_to_tail ):
				if series_oscillation[k] >= self.threshold_oscillation_aptitude:
					count_oscillation += 1
			count_oscillation = count_oscillation / ( num_market_days - self.calc_end_to_tail - tmp_start_day )
		else:
			for k in range( tmp_start_day, num_market_days - self.calc_end_to_tail ):
				count_oscillation += series_oscillation[k] / ( num_market_days - self.calc_end_to_tail - tmp_start_day )
				
		return count_oscillation
	
	@Utils.func_timer
	def save_oscillation_strength_to_db( self, method = '1' ): 
		# method = 1:calc_oscillation_strength_by_meet_aptitude_days
		# method = 2:calc_oscillation_strength_by_aptitude_expectation
		
		stock_basics = Data().get_stock_basics()
		num_stock = stock_basics.index.size
		df = pd.DataFrame( columns=( 'code', 'name', 'aptitude', 'start date', 'end date' ) )
		
		for i in range( num_stock ): 
			code = '%06d' % int( stock_basics['code'][i] )
			name = stock_basics['name'][i]
			aptitude = self.calc_oscillation_strength( code, method )				
			aptitude = '%.2f' % aptitude
			
			if self.mode == 'fixed_interval':
				start_date = Data().get_k_line_date_by_reverse_days( code, self.calc_head_to_start_or_end + self.calc_end_to_tail )
			else:
				start_date = Data().get_k_line_date_by_sequential_days( code, self.calc_head_to_start_or_end )
				
			end_date = Data().get_k_line_date_by_reverse_days( code, self.calc_end_to_tail + 1 )
			
			df.loc[i] = [ code, name, aptitude, start_date, end_date ]
			LOG( 'analyse_oscillation {0} {1:4d}/{2:4d} '.format( code, i + 1, num_stock ) )
			
		if not os.path.exists( self.path_result ):
			os.mkdir( self.path_result )
			
		Utils.save_data( df, self.path_result + 'oscillation_strength_' + self.mode + '_m' + method + '_s' + str( self.calc_head_to_start_or_end )\
			+ '_e' + str( self.calc_end_to_tail ) + '_m' + str( self.threshold_min_market_days ) + '_t' + str( self.threshold_oscillation_aptitude ) + '.xlsx' )

if __name__ == '__main__':
	
	Oscillation().save_oscillation_strength_to_db()