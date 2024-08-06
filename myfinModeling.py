from hashmap import MyHashMap
from dataframe import MyDataFrame

import requests
import pandas as pd
import re
import time
from datetime import datetime, timedelta

tickerTime = 0

def periodRangeF(colDate):
    return abs(colDate[1]-colDate[0])

def cleanDfDates(df):
    difTolerance = 3
    difTolerance = timedelta(days=difTolerance)
    one_day = timedelta(days=1)
    #Set quarter ranges
    quarterRange = 100
    quarterRange = timedelta(days=quarterRange)
    threeQuarterRange = 260
    threeQuarterRange = timedelta(days=threeQuarterRange)
    yearRange = 350
    yearRange = timedelta(days=yearRange)
    
    i = 0     
    while i < len(df.columns) - 1:
        periodRange = periodRangeF(df.columns[i])

        if (i < len(df.columns)-1):
            nextDateDiff = abs(df.columns[i+1][0]-df.columns[i][1])
        else:
            nextDateDiff = difTolerance + one_day
        
        if (periodRange > yearRange):
            if (i < 3) | (i == len(df.columns)-1):
                 df.drop(df.columns[i])
                 i=-1
            elif (abs(df.columns[i-1][1]-df.columns[i+1][0]) < nextDateDiff):
                 df.drop(df.columns[i])
                 i = i - 1
            elif nextDateDiff < difTolerance:
                if i >= 3:
                    index = df.columns.index(df.columns[i])
                    currentQuarterVal = df.data[df.columns[index]][0] - (
                        df.data[df.columns[index-1]][0] + 
                        df.data[df.columns[index-2]][0] + 
                        df.data[df.columns[index-3]][0]
                    )
                    df.data[df.columns[i]][0] = currentQuarterVal
                    currentQuarterDate = (df.columns[i-1][1] + one_day, df.columns[i][1])
                    df.rename(columns={df.columns[i]: currentQuarterDate})

        
        #column is 9 month period
        elif ((periodRange > threeQuarterRange) and (periodRange < yearRange)):
            #If the end period of the next column match up, use data to create missing quarter
            if i == 0:
                df.drop(df.columns[i])
                i=-1
            elif abs(df.columns[i+1][0]-df.columns[i-1][1]) < nextDateDiff:
                df.drop(df.columns[i])
            elif df.columns[i][1] == df.columns[i+1][1]:
                if i >= 1:
                    index = df.columns.index(df.columns[i])
                    currentQuarterVal = df.data[df.columns[index]][0] - (
                        df.data[df.columns[index-1]][0] + 
                        df.data[df.columns[index-2]][0] + 
                        df.data[df.columns[index-3]][0]
                    )
                    df.data[df.columns[i]][0] = currentQuarterVal
                    currentQuarterDate = (df.columns[i-1][1] + one_day, df.columns[i][1])
                    df.rename(columns={df.columns[i]: currentQuarterDate})
                else:
                    df.drop(df.columns[i])
                    i=-1
         
        #column is 6 month period
        elif ((periodRange > quarterRange) and (periodRange < threeQuarterRange)):
             #redudant 6 month
            if i > 0:
                prevRange = abs(df.columns[i-1][1]-df.columns[i-1][0])
                nextRange = abs(df.columns[i+1][1]-df.columns[i+1][0])
                if (prevRange < quarterRange) and (nextRange < quarterRange):
                    if (abs(df.columns[i-1][1]-df.columns[i+1][0]) < nextDateDiff):
                        df.drop(df.columns[i])
                        i = i - 1
            #If the end period of the next column match up, use data to create missing quarter
            elif df.columns[i][1] == df.columns[i+1][1]:
                index = df.columns.index(df.columns[i])
                currentQuarterVal = df.data[df.columns[index]][0] - (
                    df.data[df.columns[index-1]][0] + 
                    df.data[df.columns[index-2]][0] + 
                    df.data[df.columns[index-3]][0]
                )
                df.data[df.columns[i]][0] = currentQuarterVal
                currentQuarterDate = (df.columns[i-1][1] + one_day, df.columns[i][1])
                df.rename(columns={df.columns[i]: currentQuarterDate})
         #current column doesnt match next column sequencally 

        '''
        if (realRange > quarterRange) and (realRange < threeQuarterRange):
            if i > 0:
                prevRange = abs(colNames[i - 1][1] - colNames[i - 1][0])
                nextRange = abs(colNames[i + 1][1] - colNames[i + 1][0])
                if (prevRange < quarterRange) and (nextRange < quarterRange):
                    if abs(colNames[i - 1][1] - colNames[i + 1][0]) < dayRange:
                        del colNames[i]
                        i -= 1
            elif colNames[i][1] == colNames[i + 1][1]:
                index = df.columns.index(colNames[i])
                currentQuarterVal = df.data[colNames[i]][0] - df.data[colNames[i + 1]][0]
                df.data[colNames[i]][0] = currentQuarterVal
                currentQuarterDate = (colNames[i][0], colNames[i + 1][0] - one_day)
                df.columns = [currentQuarterDate if col == colNames[i] else col for col in df.columns]
                colNames[i] = currentQuarterDate
        
        elif (realRange > threeQuarterRange) and (realRange < yearRange):
            if i == 0:
                del colNames[0:i]
                i = -1
            elif abs(colNames[i + 1][0] - colNames[i - 1][1]) < dayRange:
                del colNames[i]
            elif colNames[i][1] == colNames[i + 1][1]:
                if i >= 1:
                    index = df.columns.index(colNames[i])
                    currentQuarterVal = df.data[colNames[i]][0] - (df.data[colNames[i - 1]][0] + df.data[colNames[i + 1]][0])
                    df.data[colNames[i]][0] = currentQuarterVal
                    currentQuarterDate = (colNames[i][0], colNames[i + 1][0] - one_day)
                    df.columns = [currentQuarterDate if col == colNames[i] else col for col in df.columns]
                    colNames[i] = currentQuarterDate
                else:
                    del colNames[0:i]
                    i = -1
        
        elif realRange > yearRange:
            prevRange = abs(colNames[i - 1][1] - colNames[i - 1][0])
            nextRange = abs(colNames[i + 1][1] - colNames[i + 1][0])
            if i < 3:
                del colNames[i]
                i = -1
            elif abs(colNames[i - 1][1] - colNames[i + 1][0]) < dayRange:
                del colNames[i]
                i -= 1
            elif dayRange < difTolerance:
                if i >= 3:
                    index = df.columns.index(colNames[i])
                    currentQuarterVal = df.data[colNames[i]][0] - (df.data[colNames[i - 1]][0] + df.data[colNames[i - 2]][0] + df.data[colNames[i - 3]][0])
                    df.data[colNames[i]][0] = currentQuarterVal
                    currentQuarterDate = (colNames[i - 1][1] + one_day, colNames[i][1])
                    df.columns = [currentQuarterDate if col == colNames[i] else col for col in df.columns]
                    colNames[i] = currentQuarterDate
                else:
                    del colNames[0:i]
                    i = -1
         '''
        i += 1
    return df




def getTickers(email):
    """
    This function retrieves all the tickers on the SEC website and returns them in a dataframe
    """
    
    headers = {'User-Agent': f"{email}"}
    startTime = time.time()
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
        )
    endTime = time.time()
    tickerTime = endTime - startTime
    tickerDict = companyTickers.json()
    tickerDf = pd.DataFrame(index = range(len(tickerDict)), columns = ["CIK", "Ticker", "Name"])
    for i in range(len(tickerDict)): 
        tickerDf.iloc[i, 0] = str(tickerDict[str(i)]['cik_str']).zfill(10)
        tickerDf.iloc[i, 1] = tickerDict[str(i)]['ticker']
        tickerDf.iloc[i, 2] = tickerDict[str(i)]['title']
    return tickerDf

def getTickers(email):
    """
    This function retrieves all the tickers on the SEC website and returns them in a dataframe
    """
    
    headers = {'User-Agent': f"{email}"}
    startTime = time.time()
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
        )
    endTime = time.time()
    tickerTime = endTime - startTime
    tickerDict = companyTickers.json()
    tickerDf = pd.DataFrame(index = range(len(tickerDict)), columns = ["CIK", "Ticker", "Name"])
    for i in range(len(tickerDict)): 
        tickerDf.iloc[i, 0] = str(tickerDict[str(i)]['cik_str']).zfill(10)
        tickerDf.iloc[i, 1] = tickerDict[str(i)]['ticker']
        tickerDf.iloc[i, 2] = tickerDict[str(i)]['title']
    return tickerDf

class Company:
    
    email = "anthonytaylor@ufl.edu"
    tickerDf = getTickers(email)

    def __init__(self, ticker):
        self.ticker = ticker
        self.cik = self.findCik()
        self.rawCompanyData = self.findRawCompanyData()
        self.incomeStatementDict = self.rawDataToIncomeStatementDict()
        self.incomeStatement = self.formIncStateFromDict()

    def findCik(self):
        cikRow = self.tickerDf[self.tickerDf['Ticker'] == self.ticker]
        cik = cikRow.iloc[0, 0]
        return cik
    
    def getCik(self):
        return self.cik 
    
    def findRawCompanyData(self):
        rawCompanyData = requests.get(
            f'https://data.sec.gov/api/xbrl/companyfacts/CIK{self.cik}.json',
            headers={'User-Agent': self.email})
        rawCompanyData = rawCompanyData.json()
        if rawCompanyData:
            return rawCompanyData['facts']['us-gaap']
        else:
            return 0
    
    def printRawCompanyDataKeys(self):
        print(self.rawCompanyData.keys())
        
    def searchRawCompanyDataKeys(self, keyword):
        matchList = []
        keyword = re.compile(keyword)
        for key in self.rawCompanyData.keys():
            if re.search(keyword, key, re.IGNORECASE):
                matchList.append(key)
        
    def rawDataToIncomeStatementDict(self):
        '''
        use regex to form condensed dictionary of just income statement items 
        '''
        incomeStatementDict = {}
        #Set revenue keywords useing re package
        #Add back - r"[Ss]ales[Rr]evenue[Nn]et"
        revenueKeys = [r"[Rr]evenues", r"[Ss]ales[Rr]evenue[Nn]et"]
        revenueKeys = [re.compile(key) for key in revenueKeys]
        #Search each possible keyWord with each item in the data until match is found
        breakLoop = False
        for keyWord in revenueKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['Revenue'] = self.rawCompanyData[key]['units']['USD']
                    breakLoop = True
                    break
            if breakLoop:
                breakLoop = False
                break
        #Do the same all the way dow nthe income statement
        costKeys = [r"[Cc]ost[Oo]f[Gs]oods[Ss]old", r'[Cc]ost[Oo]f[Rr]evenue', r'CostOfGoodsAndServicesSold']
        costKeys = [re.compile(key) for key in costKeys]
        for keyWord in costKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['CostOfGoodsSold'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break
        GPKeys = [r"[Gg]ross[Pp]rofit.*"]
        GPKeys = [re.compile(key) for key in GPKeys]
        for keyWord in GPKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['GrossProfit'] = self.rawCompanyData[key]['units']['USD']
                    break  
            if breakLoop:
                breakLoop = False
                break
        OpExKeys = [r"[Oo]perating[Ee]xpenses"]
        OpExKeys = [re.compile(key) for key in OpExKeys]
        for keyWord in OpExKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['OperatingExpenses'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break
        # Calculatatable from GP and Op Ex
        '''
        EBITKeys = [r"[Ii]ncome[Ff]rom[Oo]perations*"]
        EBITKeys = [re.compile(key) for key in EBITKeys]
        for keyWord in EBITKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['EBIT'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break    
        '''    
        #Try to get tax expense
        '''
        preTaxIncKeys = [r"[Ii]ncome[Bb]efore[Tt]axes*"]
        PreTaxIncKeys = [re.compile(key) for key in preTaxIncKeys]
        for keyWord in PreTaxIncKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['PreTaxIncome'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break
         '''    
        incomeKeys = [r"\b[Nn]et[Ii]ncome[Ll]oss"]
        incomeKeys = [re.compile(key) for key in incomeKeys]
        for keyWord in incomeKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['NetIncome'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break
        incomeKeys = [r"[Ee]arnings[Pp]er[Ss]hare[Bb]asic"]
        incomeKeys = [re.compile(key) for key in incomeKeys]
        for keyWord in incomeKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['EPS'] = self.rawCompanyData[key]['units']['USD/shares']
                    break
            if breakLoop:
                breakLoop = False
                break
        return incomeStatementDict
    
    def printIncomeStatementDictKeys(self):
        print(self.incomeStatementDict.keys())
        
    def printIncomeStatementDictKeys2(self):
        print(self.incomeStatementDict['Revenue'])

    '''
    def formIncStateFromDict(self):
        colDict = MyHashMap()
        item = 'CostOfGoodsSold'
        # iterate through dictionary to create a condensed dictionary that feeds into dataframe
        for i in range(len(self.incomeStatementDict[item])):
            #create a tuple of date times to represent a range of dates for each value 
             start = self.incomeStatementDict[item][i]['start']
             end = self.incomeStatementDict[item][i]['end']
             format = "%Y-%m-%d"
             start = datetime.strptime(start,format)
             end = datetime.strptime(end,format)
             quarter = (start.date(), end.date())
             colDict[quarter] = [self.incomeStatementDict[item][i]['val']]
        sorted_columns, sorted_values = colDict.to_dataframe_data()
        df = MyDataFrame(sorted_columns, sorted_values)
        return df
        '''  
    
    def formIncStateFromDict(self):
        colDict = MyHashMap()
        item = ['Revenue', 'CostOfGoodsSold', 'NetIncome', 'EPS']
        # iterate through dictionary to create a condensed dictionary that feeds into dataframe
        for i in range(len(self.incomeStatementDict[item[0]])):
            #create a tuple of date times to represent a range of dates for each value 
             start = self.incomeStatementDict[item[0]][i]['start']
             end = self.incomeStatementDict[item[0]][i]['end']
             format = "%Y-%m-%d"
             start = datetime.strptime(start,format)
             end = datetime.strptime(end,format)
             quarter = (start.date(), end.date())
             colDict[quarter] = [self.incomeStatementDict[item[0]][i]['val']]
        sorted_columns, sorted_values = colDict.to_dataframe_data()
        revenueDf = MyDataFrame(sorted_columns, sorted_values)
        revenueDf = cleanDfDates(revenueDf)
        '''
        for i in range(len(self.incomeStatementDict[item[1]])):
            #create a tuple of date times to represent a range of dates for each value 
             start = self.incomeStatementDict[item[1]][i]['start']
             end = self.incomeStatementDict[item[1]][i]['end']
             format = "%Y-%m-%d"
             start = datetime.strptime(start,format)
             end = datetime.strptime(end,format)
             quarter = (start.date(), end.date())
             colDict[quarter] = [self.incomeStatementDict[item[1]][i]['val']]
        '''
        incomeDf = revenueDf
        return incomeDf

    def printIncState(self):
        pd.set_option('display.max_rows', None)
        print(self.incomeStatement)
            
    
    
    





# tsla = Company("TSLA")
# tsla.printRawCompanyDataKeys()
# tsla.printIncomeStatementDictKeys()
        
aapl = Company("TSLA")
# aapl.printRawCompanyDataKeys()
aapl.printIncState()
# aapl.printIncomeStatementDictKeys2()

#Keys of inital dictionary that conatins all data for one company
#Output should look like ['cik', 'entityName', 'facts']
#Facts contains all data and is a dictionary
#cik and entityName are just the name and ID of company
#print(tsla.findCompanyFacts().keys())

#the keys of the facts are ['dei', 'us-gaap'] 
#dei is irrelavlent and we need what is in us-gaap, which is a also a dictionary
#print(tsla.findCompanyFacts()['facts'].keys())

#the keys of facts are all of the line items of every financial statement that the sec keeps data on
#print(tsla.findCompanyFacts()['facts']['us-gaap'].keys())

#here I choose the first line item to look at
#every line item is a dictionary and should have keys ['label', 'description', 'units']
#label and decription only conatain a string about the line item
#units is a dictionary with one key, ['USD']
#print(tsla.findCompanyFacts()['facts']['us-gaap']['AccountsAndNotesReceivableNet'].keys())

#USD is a list
#the first element of the list is the data relating to the oldest continuously stored data point of the line item
#and each element after that should be the data relating to the line item 1 quarter later 
#print(tsla.findCompanyFacts()['facts']['us-gaap']['AccountsAndNotesReceivableNet']['units']['USD'])






