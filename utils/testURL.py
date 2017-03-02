# -*- coding:utf-8 -*- 

import urllib.request

url = "http://vip.stock.finance.sina.com.cn/q/go.php/vFinanceAnalyze/kind/mainindex/index.phtml?s_i=&s_a=&s_c=&reportdate=2016&quarter=1&p=1&num=40"
 
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}#,'Accept':'text/html;q=0.9,*/*;q=0.8','Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3','Accept-Encoding':'gzip','Connection':'close','Referer':None}

req = urllib.request.Request(url, headers = headers)  
try: 
	myPage = urllib.request.urlopen(req).read(70000)
except:
	pass
'''
opener = urllib.request.build_opener()
opener.add_headers = [headers]
data = opener.open(url).read()
'''
print(myPage)
