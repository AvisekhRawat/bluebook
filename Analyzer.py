import json
import pandas as pd

class Analyzer:
	def __init__(self):
		print('constructor')

	def analyze(self):
		sdmD = ''
		with open('SDMDict.json') as sdmDict:
			sdmD = json.load(sdmDict) 			
			print(len(sdmD))
		df = pd.DataFrame(columns = ['SDMName'])		
		row = 0
		l = []			
		for sdm in sdmD:
			print(sdm)
			l.append(sdm)			
			
		print('Putting in excel')		
		l.sort()
		for row in range(len(l)):
			print(l[row])			
			df[row] = l[row]		
		print(df.T.shape)		
		df.T.to_excel('allSDM.xls')		


if(__name__ == '__main__'):
	print('start')
	a = Analyzer()
	a.analyze()
