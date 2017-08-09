import urllib,csv,requests,os
from requests import session
from threading import Thread

start_year=1998
start_month=1
base='http://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"zb","valuecode":"'
suffix='"},{"wdcode":"sj","valuecode":"%s0%s-"}]'%(str(start_year),str(start_month))

Codedict={
"Price indices":
{'A01010101': 'Consumer Price Index (The same month last year=100)', 
'A01080101': 'Producer Price Index for Industrial Products (The same _month last year=100)', 
'A01010201': 'Consumer Price Index (The same month last year=100)_old', 
'A01040101': 'Retail Price Index (The same month last year=100)'},

#"Fixed Assets Investment_Total and components":
#{'A130C01': 'Investment in Private Fixed Assets, Accumulated(100 million yuan)',
#'A130101': 'Investment Actually Completed in Fixed Assets, Accumulated(100 million yuan)', 
#'A13070B01':'Investment in Fixed Assets, Real Estate, Accumulated',
#"A13060B01":"Investment in Fixed Assets, Real Estate, Accumulated_old",
#"A13070301":"Investment in Fixed Assets, Manufacturing, Accumulated",
#"A13060301":"Investment in Fixed Assets, Manufacturing, Accumulated_old",


"Real Estate Floor Space database": 
{'A140A01': 'Floor Space of Commercialized Residential Buildings _Sold, Accumulated',
'A140A05': 'Floor Space of Commercialized Residential Buildings _Sold, forward delivery housing, Accumulated',
'A140A03': 'Floor Space of Commercialized Residential Buildings _Sold, complete dapartment, Accumulated',
'A140503':'Newly Started Floor Space of Commercialized Residential Buildings, Accumulated',
'A140205':'Sources of Funds of Enterprises for Real Estate _Development, Accumulated',
'A140105':'Total Investment in Residential Buildings in Real _Estate Development, Accumulated',
'A140101': 'Real estate investment - Cumulative Value ($100 million)',
},

#"Land area purchases, investment growth and RE total":
#{"A140205":"Sources of Funds of Enterprises for Real Estate _Development, Accumulated",
# 'A140301': 'Development and Sales of Real Estate, Land Space _Purchased, Accumulated',
#'A130105':"Investment Actually Completed by Enterprises for Real _Estate Development, Accumulated",
#'A140401': 'Floor Space of Real Estate Under Construction, _Accumulated', 
#'A140403': 'Floor Space of Real Estate Started This Year, _Accumulated', 
#'A140405': 'Floor Space of Real Estate Completed, Accumulated',
#'A140303': 'Development and Sales of Real Estate, Transaction _Value of Land, Accumulated', 
#},

#"Financial and Fiscal":
#{'A1A0101': 'Government Revenue, Current Period',
# 'A1B0103': 'Money (M1) Supply, period-end', 
# 'A1B0101': 'Money and Quasi-Money (M2) Supply, period-end', 
# 'A1A0201': 'Government Expenditure , Current Period', 
 #'A1B0105': 'Currency in Circulation (M0) Supply, period-end'},
#"Ex_import, FDI, Industrial profit, whole sales":
#{'A160105': 'Value of Exports, Current Period',
# 'A150101': 'Retail Sales of Consumer Goods, Current Period', 
# 'A16020B': 'Foreign Direct Investment Actually Utilized, _Accumulated', 
# 'A160109': 'Value of Imports, Current Period',
#'A02090N': 'Revenue from Principal Business, Accumulated', 
#'A02090H': 'Total Assets, Accumulated',
#'A020J18': 'Total Profits of Private Industrial Enterprises, _Accumulated ',
#'A020918': 'Total Profits, Accumulated ', 
#'A170205':'Freight Ton-Kilometers of Railways, Current Period',
 #'A02091A': 'Total Profits, Accumulated Growth Rate', 'A020915': 'Interest Expenses, Accumulated '
#'A020101': 'Value-added of Industry, Growth Rate (The same period _last year=100)',
#'A020912': 'Financial Costs, Accumulated ', 'A02090K': 'Total Liabilities, Accumulated',
#'A02090P': 'Revenue from Principal Business, Accumulated Growth _Rate',
}
class MyThread(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None

    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
    def join(self):
        Thread.join(self)
        return self._return

# THIS PART RETRIEVES TIME INDEX
with session() as c:
	val = c.post(base+'A130C01'+suffix).content.split('{"cn')
	time_index=['']+['%s-01-%s'%(start_month+i%12,start_year+i//12) for i in xrange (len(val)-2)]
	print time_index

def fetch_url(code,key):
	with session() as c:
		return([Codedict[key][code]]+c.post(base+code+suffix).content.split('{"data":')[1:][::-1]) #arranging data in chronological order 
	
def spin(key):
	threads = [MyThread(target=fetch_url, args=(code,key)) for code in Codedict[key]]
	for thread in threads:
	    thread.start()
	with open('Data Import/%s.csv'%key, 'w') as f:
		writer=csv.writer(f,delimiter=',',lineterminator='\n')
	
		writer.writerows(gener([strand.join() for strand in threads]))
	print 'Updated.......%s.csv' %key

def gener(data):
	for count in xrange (len(time_index)):
		if count==0:
			
			yield [time_index[count]]+[a[count] for a in data]
		else:
			yield [time_index[count]]+[a[count][:a[count].index(',')] for a in  data]

threads = [Thread(target=spin, args=(key,)) for key in Codedict]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
