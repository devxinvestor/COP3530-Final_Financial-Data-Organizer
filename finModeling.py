
import requests
import pandas as pd



def getTickers(email):
    """
    This function retrieves all the tickers on the SEC website and returns them in a dataframe
    """
    headers = {'User-Agent': f"{email}"}
    companyTickers = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
        )

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
        self.companyFacts = self.findCompanyFacts()

    def findCik(self):
        cikRow = self.tickerDf[self.tickerDf['Ticker'] == self.ticker]
        cik = cikRow.iloc[0, 0]
        return cik
    
    def getCik(self):
        return self.cik 
    
    def findCompanyFacts(self):
        companyFacts = requests.get(
            f'https://data.sec.gov/api/xbrl/companyfacts/CIK{self.cik}.json',
            headers={'User-Agent': self.email})
        companyFacts = companyFacts.json()
        return companyFacts

class Data:
        
        def __init__(self, company):
            self.company = company
            self.data = self.company.findCompanyFacts()['facts']['us-gaap']
        
        def getLineItems(self):
            return self.data.keys()
        
        def getLineItemData(self, lineItem):
            return self.data[lineItem]['units']['USD']
        
        def getLineItemDataPoint(self, lineItem, quarter):
            return self.getLineItemData(lineItem)[quarter]





tickerdf = getTickers("anthonytaylor@ufl.edu")
for tickers in tickerdf['Ticker'][1]:
    company = Company(tickers)
    print(company.findCompanyFacts()['facts']['us-gaap'].keys())
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






