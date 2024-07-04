from datetime import datetime

import pandas as pd
from decimal import Decimal
from classes.functions import Functions as f
import classes.pandas as pandas


def command_parser(run, config):
    if run["type"] == "IBKR":
        from classes.parser_IBKR import write_Entries
        outputFile = run["output"]
        write_Entries(outputFile, config)
        pass
    elif run["type"] == "n26":
        from classes.parser_n26 import write_Entries
        outputFile = run["output"]
        write_Entries(outputFile, config)
        pass
    elif run["type"] == "YahooFinance":
        pass
    pass

def command_merge(run, config):
    entries = []
    input_1 = run["input_1"]
    input_2 = run["input_2"]
    output = run["output"]
    separator = config["CSV_Separator"]

    entries_1 = pandas.read_file(input_1,separator )
    entries_2 = pandas.read_file(input_2,separator )


    entries = pd.concat( [entries_1, entries_2])
    entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
    pandas.write_file_entries(entries,output,separator)
    pass

def command_benchmark(run, config):
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    entries = pandas.read_file(input,separator )
    entries_filtered = entries.loc[entries['Type'] == "Transaction"]
    entries_filtered = entries_filtered.loc[entries_filtered['Account'] == run["benchmark"]]
    entries_filtered = entries_filtered.loc[entries_filtered['ID'].str.contains("IBKR_")]
    benchmarkTicker = run["benchmarkTicker"]
    maxDepth =  run["maxDepth"]

    for index, row in entries_filtered.iterrows():
        date = row["Date"]
        price = f.get_price(entries, benchmarkTicker, row["Quantity_Type"], 0, maxDepth, row["Date"])
        if price == None:
            quantity = 0
        else:
            quantity = round( Decimal( row["Quantity"]) / Decimal(price))

        benchmarkEntry = pd.DataFrame( [{
            "Date": date,
            "Type": "Benchmark",
            "ID": "Benchmark_" + str(f.generate_unique_uuid),
            "Name": benchmarkTicker,
            "Account": "Benchmark",
            "Quantity": Decimal( quantity).copy_negate(),
            "Quantity_Type": benchmarkTicker,
            "Cost": Decimal( row["Quantity"]).copy_negate(),
            "Cost_Type": row["Quantity_Type"],
        }])

        entries = pd.concat([entries, benchmarkEntry], ignore_index=True)
    pandas.write_file_entries(entries,output,separator)

    pass

def command_balance(run, config):
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    increment = run["increment"]
    entries = pandas.read_file(input,separator )
    entries["Year"] = entries["Date"].dt.year
    entries["Quarter"] = entries["Date"].dt.quarter
    entries["Month"] = entries["Date"].dt.month
    fairValue = run["fairValue"]

    if run["fairValue"]:
        fairValueCurrency = run["fairValueCurrency"]
        fairValueDate = run["fairValueDate"]
        entries = entries.loc[entries['Date'] <= fairValueDate]
        prices = f.get_priceUpdates(entries)

    if increment == "Latest":
        e = entries[["Account","Quantity_Type","Quantity"]]
        balance = e.groupby(["Account", "Quantity_Type"]).sum().reset_index()

    elif increment == "Year":
        e = entries[["Year","Account","Quantity_Type","Quantity"]]
        balance = e.groupby(["Year","Account", "Quantity_Type"]).sum().reset_index()

    elif increment == "Quarter":
        e = entries[["Year","Quarter","Account","Quantity_Type","Quantity"]]
        balance = e.groupby(["Year","Quarter","Account", "Quantity_Type"]).sum().reset_index()

    elif increment == "Month":
        e = entries[["Year","Month","Account","Quantity_Type","Quantity"]]
        balance = e.groupby(["Year","Month","Account", "Quantity_Type"]).sum().reset_index()

    if fairValue:
       balance["Fair Value"] = balance.apply( lambda x: Decimal( x["Quantity"] ) * Decimal( f.get_price(prices,x["Quantity_Type"], fairValueCurrency, 0, 5 ) ), axis=1)

    pandas.write_file_balance(balance,output,separator)

    pass


def command_runningTotal(run, config):
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])

    startDate = f.get_runParameter(run, "startDate")
    endDate = f.get_runParameter(run, "endDate")
    increment = f.get_runParameter(run, "increment")
    AccountFilter = f.get_runParameter(run, "AccountFilter")
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")


    entries = pandas.read_file(input,separator )
    entries = f.get_transactions(entries)

    if startDate == None:
        startDate = entries["Date"].min()

    if endDate == None:
        endDate = entries["Date"].max()

    if AccountFilter != None:
        account = entries[entries['Account'].str.contains(AccountFilter, na=False)]
    else:
        account = entries

    runningTotalFrame = pandas.get_crossJoinedFrames(pandas.get_dateFrame(startDate, endDate, increment),
                                                     pandas.get_uniqueFrame(account, "Account"))

    runningTotalFrame = pandas.get_crossJoinedFrames(runningTotalFrame,
                                                     pandas.get_uniqueFrame(entries, "Quantity_Type"))

    runningTotalFrame = runningTotalFrame.dropna(subset=["Account", "Date", "Quantity_Type"])

    change = entries.groupby(by=[pd.Grouper(key='Date', freq=increment), "Account", "Quantity_Type"])["Quantity"].sum()
    result = runningTotalFrame.merge(change, how="left", on=["Date", "Account", "Quantity_Type"])
    result = result.fillna(0)
    result = pandas.get_cumulativesum(result)

    prices = f.get_priceUpdates(entries)
    if fairValueCurrency != None:
        result["Price"] = result.apply(lambda x: f.get_LatestPrice(prices, x["Date"], x["Quantity_Type"], fairValueCurrency, 0,5  ), axis=1)
        result["RunningTotal_FairValue"] = result.apply(lambda x: x["Price"] * x["RunningTotal"], axis=1)
        outputList = result[
            ["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal", "Price", "RunningTotal_FairValue"]].rename(
            columns={
                "Quantity": "Change"
            })
    else:
        outputList = result[["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal"]].rename(
        columns={
            "Quantity": "Change"
        } )

    pandas.write_file_balance(outputList,output,separator)

    pass

class Commands:

    def run_command(run, config, functions):

        if run["task"] == "parser":
            command_parser(run, config)
        elif run["task"] == "merge":
            command_merge(run, config)
            pass
        elif run["task"] == "benchmark":
            command_benchmark(run, config)
            pass
        elif run["task"] == "balance":
            command_balance(run, config)
            pass
        elif run["task"] == "runningTotal":
            command_runningTotal(run, config)
            pass
        else:
            print("Other")
