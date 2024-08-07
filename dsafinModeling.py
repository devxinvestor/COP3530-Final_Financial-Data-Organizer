from hashmap import MyHashMap
from dataframe import MyDataFrame

import requests
import pandas as pd
import re
import time
from datetime import datetime, timedelta

def homemadePeriodRangeF(colDate):
    return abs(colDate[1]-colDate[0])

def homemadeCleanDfDates(df):
    difTolerance = 3
    difTolerance = timedelta(days=difTolerance)
    one_day = timedelta(days=1)
    quarterRange = timedelta(days=100)
    threeQuarterRange = timedelta(days=260)
    yearRange = timedelta(days=350)

    i = 0
    while i < len(df.columns) - 1:
        periodRange = homemadePeriodRangeF(df.columns[i])

        if i < len(df.columns) - 1:
            nextDateDiff = abs(df.columns[i + 1][0] - df.columns[i][1])
        else:
            nextDateDiff = difTolerance + one_day

        if periodRange > yearRange:
            if i < 3 or i == len(df.columns) - 1:
                df.drop(df.columns[i])
                i = -1
            elif abs(df.columns[i - 1][1] - df.columns[i + 1][0]) < nextDateDiff:
                df.drop(df.columns[i])
                i -= 1
            elif nextDateDiff < difTolerance:
                if i >= 3:
                    index = df.columns.index(df.columns[i])
                    currentQuarterVal = (
                        df.data[df.columns[index]][0] - 
                        df.data[df.columns[index - 1]][0] - 
                        df.data[df.columns[index - 2]][0] - 
                        df.data[df.columns[index - 3]][0]
                    )
                    df.data[df.columns[i]][0] = currentQuarterVal
                    currentQuarterDate = (df.columns[i - 1][1] + one_day, df.columns[i][1])
                    df.rename({df.columns[i]: currentQuarterDate})

        elif threeQuarterRange < periodRange < yearRange:
            if i == 0:
                df.drop(df.columns[i])
                i = -1
            elif abs(df.columns[i + 1][0] - df.columns[i - 1][1]) < nextDateDiff:
                df.drop(df.columns[i])
            elif df.columns[i][1] == df.columns[i + 1][1]:
                if i >= 1:
                    index = df.columns.index(df.columns[i])
                    currentQuarterVal = (
                        df.data[df.columns[index]][0] - 
                        df.data[df.columns[index - 1]][0] - 
                        df.data[df.columns[index - 2]][0] - 
                        df.data[df.columns[index - 3]][0]
                    )
                    df.data[df.columns[i]][0] = currentQuarterVal
                    currentQuarterDate = (df.columns[i - 1][1] + one_day, df.columns[i][1])
                    df.rename({df.columns[i]: currentQuarterDate})
                else:
                    df.drop(df.columns[i])
                    i = -1

        elif quarterRange < periodRange < threeQuarterRange:
            if i > 0:
                prevRange = abs(df.columns[i - 1][1] - df.columns[i - 1][0])
                nextRange = abs(df.columns[i + 1][1] - df.columns[i + 1][0])
                if prevRange < quarterRange and nextRange < quarterRange:
                    if abs(df.columns[i - 1][1] - df.columns[i + 1][0]) < nextDateDiff:
                        df.drop(df.columns[i])
                        i -= 1
            elif df.columns[i][1] == df.columns[i + 1][1]:
                index = df.columns.index(df.columns[i])
                currentQuarterVal = (
                    df.data[df.columns[index]][0] - 
                    df.data[df.columns[index - 1]][0] - 
                    df.data[df.columns[index - 2]][0] - 
                    df.data[df.columns[index - 3]][0]
                )
                df.data[df.columns[i]][0] = currentQuarterVal
                currentQuarterDate = (df.columns[i - 1][1] + one_day, df.columns[i][1])
                df.rename({df.columns[i]: currentQuarterDate})

        i += 1
    return df

def homemadeMergeDf(df1, df2):
    # Extract column names from df1 and df2 in order
    columns_df1 = df1.columns
    columns_df2 = df2.columns
    
    # Initialize new data storage for merged data
    merged_data = MyHashMap()
    merged_columns = []

    # Add columns and data from df1
    for col in columns_df1:
        col_data = df1[col]
        # Exclude the column if all values are None
        if all(value is None for value in col_data):
            continue
        # Exclude the column if it does not exist in df2
        if col not in columns_df2:
            continue
        merged_data[col] = col_data
        merged_columns.append(col)
    
    # Add columns and data from df2 if not already in merged_columns
    for col in columns_df2:
        col_data = df2[col]
        # Exclude the column if all values are None
        if all(value is None for value in col_data):
            continue
        if col not in merged_columns:
            merged_data[col] = [None] * df1.row_count() + col_data
            merged_columns.append(col)
        else:
            merged_data[col].extend(col_data)
    
    # Convert merged_data to sorted_columns and sorted_values for MyDataFrame
    merged_sorted_columns, merged_sorted_values = merged_data.to_dataframe_data()
    
    # Create a new MyDataFrame with the merged data
    merged_df = MyDataFrame(sorted_columns=merged_sorted_columns, sorted_values=merged_sorted_values)
    
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

class HomemadeCompany:
    
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

        EBITKeys = [r"[Ii]ncome[Ff]rom[Oo]perations*", r"IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", r"IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"]
        EBITKeys = [re.compile(key) for key in EBITKeys]
        for keyWord in EBITKeys:
            for key in self.rawCompanyData.keys():
                if keyWord.search(key):
                    incomeStatementDict['EBIT'] = self.rawCompanyData[key]['units']['USD']
                    break
            if breakLoop:
                breakLoop = False
                break    
    
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
            item = ['Revenue', 'CostOfGoodsSold', 'EBIT', 'NetIncome', 'EPS']
            lineItems = ['Revenue', 'CostOfGoodsSold', 'Gross Profit', 'OperatingExpenses', 'Income From Operations', 'NetIncome', 'EPS']

            revDict = MyHashMap()
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
            rev_sorted_columns, rev_sorted_values = revDict.to_dataframe_data()
            revenueDf = MyDataFrame(rev_sorted_columns, rev_sorted_values)
            revenueDf = homemadeCleanDfDates(revenueDf)

            cogDict = MyHashMap()
            # iterate through dictionary to create a condensed dictionary that feeds into dataframe
            for i in range(len(self.incomeStatementDict[item[1]])):
                #create a tuple of date times to represent a range of dates for each value 
                start = self.incomeStatementDict[item[1]][i]['start']
                end = self.incomeStatementDict[item[1]][i]['end']
                format = "%Y-%m-%d"
                start = datetime.strptime(start,format)
                end = datetime.strptime(end,format)
                quarter = (start.date(), end.date())
                cogDict[quarter] = [self.incomeStatementDict[item[1]][i]['val']]
            cog_sorted_columns, cog_sorted_values = cogDict.to_dataframe_data()
            cogDf = MyDataFrame(cog_sorted_columns, cog_sorted_values)
            cogDf = homemadeCleanDfDates(cogDf)
            incomeDf = homemadeMergeDf(revenueDf,cogDf)
            incomeDf.drop(incomeDf.columns[-1])

            second_last_row = incomeDf.get_row(incomeDf.row_count()-2)
            last_row = incomeDf.get_row(incomeDf.row_count()-1)
            difference = [second_last - last for second_last, last in zip(second_last_row, last_row)]
            incomeDf.add_row(difference)

            opInDict = MyHashMap()
            for i in range(len(self.incomeStatementDict[item[2]])):
                #create a tuple of date times to represent a range of dates for each value 
                start = self.incomeStatementDict[item[2]][i]['start']
                end = self.incomeStatementDict[item[2]][i]['end']
                format = "%Y-%m-%d"
                start = datetime.strptime(start,format)
                end = datetime.strptime(end,format)
                quarter = (start.date(), end.date())
                opInDict[quarter] = [self.incomeStatementDict[item[2]][i]['val']]
            op_sorted_columns, op_sorted_values = opInDict.to_dataframe_data()
            opDf = MyDataFrame(op_sorted_columns, op_sorted_values)
            opDf = homemadeCleanDfDates(opDf)
            incomeDf = homemadeMergeDf(incomeDf,opDf)
            incomeDf.drop(incomeDf.columns[-1])

            take_two_second_last_row = incomeDf.get_row(incomeDf.row_count()-2)
            take_two_last_row = incomeDf.get_row(incomeDf.row_count()-1)
            take_two_difference = [second_last - last for second_last, last in zip(take_two_second_last_row, take_two_last_row)]
            incomeDf.add_row(take_two_difference)

            netDict = MyHashMap()
            for i in range(len(self.incomeStatementDict[item[3]])):
                #create a tuple of date times to represent a range of dates for each value 
                 start = self.incomeStatementDict[item[3]][i]['start']
                 end = self.incomeStatementDict[item[3]][i]['end']
                 format = "%Y-%m-%d"
                 start = datetime.strptime(start,format)
                 end = datetime.strptime(end,format)
                 quarter = (start.date(), end.date())
                 netDict[quarter] = [self.incomeStatementDict[item[3]][i]['val']]
            net_sorted_columns, net_sorted_values = netDict.to_dataframe_data()
            netDf = MyDataFrame(net_sorted_columns, net_sorted_values)
            netDf = homemadeCleanDfDates(netDf)
            incomeDf = homemadeMergeDf(incomeDf,netDf)

            incomeDf = revenueDf
            return incomeDf
        else:
            return -1

    def printIncState(self):
        print(self.incomeStatement)