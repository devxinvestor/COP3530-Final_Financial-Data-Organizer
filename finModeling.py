
import requests
import pandas as pd
import re
import time
from datetime import datetime, timedelta

tickerTime = 0

def periodRangeF(colDate):
    return abs(colDate[1]-colDate[0])

def cleanDfDates(df):
    df.columns = df.columns.tolist()
    #Set day tolerance from one quarter to next
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
    
    #Iterate through all dates to ensure one after the other format 
    i = 0     
    while i < len(df.columns):
         periodRange = periodRangeF(df.columns[i]) 
         
         if (i < len(df.columns)-1):
             nextDateDiff = abs(df.columns[i+1][0]-df.columns[i][1])
         else:
             nextDateDiff = difTolerance + one_day
         
         #column is 1 year period
         if (periodRange > yearRange):
             if (i < 3) | (i == len(df.columns)-1):
                 df = df.drop(df.columns[0:i+1],axis=1)
                 i=-1
             #redudant year col
             elif (abs(df.columns[i-1][1]-df.columns[i+1][0]) < nextDateDiff):
                 df = df.drop(df.columns[i],axis=1)
                 i = i - 1    
             #If the end period of the next column match up, use data to create missing quarter
             elif nextDateDiff < difTolerance:
                if i >= 3:
                    index = df.columns.get_loc(df.columns[i])
                    currentQuarterVal = df.iloc[0,index] - (df.iloc[0,index-1] + df.iloc[0,index-2] + df.iloc[0,index-3])
                    df.iloc[0,i] = currentQuarterVal
                    currentQuarterDate = (df.columns[i-1][1] + one_day, df.columns[i][1])
                    df = df.rename(columns={df.columns[i]: currentQuarterDate})
                else:
                    df = df.drop(df.columns[0:i+1],axis=1)
                    i=-1
                    
         #column is 9 month period
         elif ((periodRange > threeQuarterRange) and (periodRange < yearRange)):
             #If the end period of the next column match up, use data to create missing quarter
             if i == 0:
                 df = df.drop(df.columns[i],axis=1)
                 i=-1
             elif abs(df.columns[i+1][0]-df.columns[i-1][1]) < nextDateDiff:
                 df = df.drop(df.columns[i],axis=1)
             elif df.columns[i][1] == df.columns[i+1][1]:
                if i >= 1:
                    index = df.columns.get_loc(df.columns[i])
                    currentQuarterVal = df.iloc[0,index] - (df.iloc[0,index-1] + df.iloc[0,index+1])
                    df.iloc[0,i] = currentQuarterVal
                    currentQuarterDate = (df.columns[i][0], df.columns[i+1][0] - one_day)
                    df = df.rename(columns={df.columns[i]: currentQuarterDate})
                else:
                    df = df.drop(df.columns[i],axis=1)
                    i=-1
                    
         #column is 6 month period
         elif ((periodRange > quarterRange) and (periodRange < threeQuarterRange)):
             #redudant 6 month
             if i > 0:
                 prevRange = abs(df.columns[i-1][1]-df.columns[i-1][0])
                 nextRange = abs(df.columns[i+1][1]-df.columns[i+1][0])
                 if (prevRange < quarterRange) and (nextRange < quarterRange):
                     if (abs(df.columns[i-1][1]-df.columns[i+1][0]) < nextDateDiff):
                         df = df.drop(df.columns[i],axis=1)
                         i = i - 1
             #If the end period of the next column match up, use data to create missing quarter
             elif df.columns[i][1] == df.columns[i+1][1]:
                index = df.columns.get_loc(df.columns[i])
                currentQuarterVal = df.iloc[0,index] - df.iloc[0,index+1]
                df.iloc[0,i] = currentQuarterVal
                currentQuarterDate = (df.columns[i][0], df.columns[i+1][0] - one_day)
                df = df.rename(columns={df.columns[i]: currentQuarterDate})
         #current column doesnt match next column sequencally 
         '''           
         elif dayRange > difTolerance:
            #check if column i+2 is continous with i, if so delete i+1, we likely have redudant info
            dayRangeNext = abs(colNames[i+2][0]-colNames[i][1])
            if dayRangeNext < difTolerance:
                print("Here to delete: ",colNames[i+1])
                del colNames[i+1]
            #if i+2 is not continous with i we likely have a 1 or 6 month year data point and we must extract the current quarter data using the prevous quarters
            else:
                #redudant data is often missing second date, this if statement filters it out to avoid error
                if len(colNames[i+1]) == 1:
                    del colNames[i+1]
                #If there is not 3 prevous entries following the 10k data we will not be able recover current quarter data
                elif i >= 3:
                    currentQuarterVal = df.iloc[0,i+1] - (df.iloc[0,i] + df.iloc[0,i-1] + df.iloc[0,i-2])
                    df.iloc[0,i+1] = currentQuarterVal
                    currentQuarterDate = (colNames[i][1] + one_day, colNames[i+1][1])
                    #colNames[i+1] = currentQuarterDate
                    #df.columns = [currentQuarterDate if j == (i+1) else name for j, name in enumerate(df.columns)]
                else:
                    del colNames[0:i+1]
                    i = 0
         '''
        #check last item to ensure 30 day range
         i = i + 1
    return df

def mergeDf(df1, df2):
    columnNames = df1.columns
    df2 = df2[columnNames]
    df1.reset_index(drop=True, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    df1.columns = pd.Index([f"{col}_1" if df1.columns.duplicated()[i] else col for i, col in enumerate(df1.columns)])
    df2.columns = pd.Index([f"{col}_2" if df2.columns.duplicated()[i] else col for i, col in enumerate(df2.columns)])
    merged_df = pd.concat([df1, df2], axis=0,ignore_index=True)
    return merged_df

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
        '''
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
        '''    
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
        
    def formIncStateFromDict(self):
        if len(self.incomeStatementDict) == 5:
            revDict = {}
            item = ['Revenue', 'CostOfGoodsSold', 'OperatingExpenses', 'NetIncome', 'EPS']
            lineItems = ['Revenue', 'CostOfGoodsSold', 'Gross Profit', 'OperatingExpenses', 'Income From Operations', 'NetIncome', 'EPS']
            # iterate through dictionary to create a condensed dictionary that feeds into dataframe
            for i in range(len(self.incomeStatementDict[item[0]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[0]][i]['start']
                 end = self.incomeStatementDict[item[0]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 revDict[quarter] = [self.incomeStatementDict[item[0]][i]['val']]
            revenueDf = pd.DataFrame(revDict)
            revenueDf = cleanDfDates(revenueDf)
            incomeDf = revenueDf
            
            cogDict = {}
            for i in range(len(self.incomeStatementDict[item[1]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[1]][i]['start']
                 end = self.incomeStatementDict[item[1]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 cogDict[quarter] = [self.incomeStatementDict[item[1]][i]['val']]
            cogsDf = pd.DataFrame(cogDict)
            cogsDf = cleanDfDates(cogsDf)
            incomeDf = mergeDf(revenueDf,cogsDf)
            
            #create row for gross profit
            difference = incomeDf.iloc[-2] - incomeDf.iloc[-1]
            difference_df = pd.DataFrame(difference).T
            incomeDf = pd.concat([incomeDf, difference_df], ignore_index=True)
            
            #create row for operating expense
            opexDict = {}
            for i in range(len(self.incomeStatementDict[item[2]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[2]][i]['start']
                 end = self.incomeStatementDict[item[2]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 opexDict[quarter] = [self.incomeStatementDict[item[2]][i]['val']]
            opExDf = pd.DataFrame(opexDict)
            opExDf = cleanDfDates(opExDf)
            incomeDf = mergeDf(incomeDf,opExDf)
            
            #create row for income from op
            difference = incomeDf.iloc[-2] - incomeDf.iloc[-1]
            difference_df = pd.DataFrame(difference).T
            incomeDf = pd.concat([incomeDf, difference_df], ignore_index=True)
            
            #create net income row
            netDict = {}
            for i in range(len(self.incomeStatementDict[item[3]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[3]][i]['start']
                 end = self.incomeStatementDict[item[3]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 netDict[quarter] = [self.incomeStatementDict[item[3]][i]['val']]
            netIncomeDf = pd.DataFrame(netDict)
            netIncomeDf = cleanDfDates(netIncomeDf)
            incomeDf = mergeDf(incomeDf,netIncomeDf)
            
            '''
            #create eps row
            epsDict={}
            for i in range(len(self.incomeStatementDict[item[4]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[4]][i]['start']
                 end = self.incomeStatementDict[item[4]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 epsDict[quarter] = [round(self.incomeStatementDict[item[4]][i]['val'],2)]
            epsDf = pd.DataFrame(epsDict)
            epsDf = cleanDfDates(epsDf)
            incomeDf = mergeDf(incomeDf,epsDf)
            '''
            
            #add row names
            incomeDf.index = lineItems[0:6]
            return incomeDf
        else:
            return -1

    def printIncState(self):
        pd.set_option('display.max_columns', None)
        print(self.incomeStatement)
            
   
    
    





# tsla = Company("TSLA")
# tsla.printRawCompanyDataKeys()
# tsla.printIncomeStatementDictKeys()
pd.set_option('display.max_columns', None)
        
aapl = Company("TSLA")
aapl.printIncState()

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







