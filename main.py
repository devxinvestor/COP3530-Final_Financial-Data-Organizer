from finModeling import *
from dsafinModeling import *
import pandas as pd
import time

def main():
    pd.set_option('display.max_columns', None)
    
    tickers = []
    
    for _ in range(20):
        user_input = input("Enter a ticker for historical income statements (ALL CAPS) or type -1 to exit: ")
        if user_input == "-1":
            print("Exiting.")
            break
        if not user_input:
            print("No input provided. Please enter a valid ticker.")
            continue
        tickers.append(user_input)

    homemade_start_time = time.time()
    
    for ticker in tickers:
        tick = HomemadeCompany(ticker)
        print(ticker)
        tick.printIncState()

    homemade_end_time = time.time()
    homemade_elapsed_time = homemade_end_time - homemade_start_time

    pandas_start_time = time.time()
    
    for ticker in tickers:
        tick = Company(ticker)
        print(ticker)
        tick.printIncState()

    pandas_end_time = time.time()
    pandas_elapsed_time = pandas_end_time - pandas_start_time

    print(f"\nTime taken for homemade data structures: {homemade_elapsed_time:.2f} seconds")
    print(f"Time taken for built-in data structures: {pandas_elapsed_time:.2f} seconds\n")

if __name__ == "__main__":
    main()