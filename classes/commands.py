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
        priceChanges = f.get_priceUpdates(entries)
        price = f.get_LatestPrice(priceChanges,row["Date"], benchmarkTicker, row["Quantity_Type"], 0, maxDepth)
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

    e = pandas.read_file(input,separator )
    PriceChanges = f.get_priceUpdates(e)
    entries = f.get_transactions(e)

    AccountFilter = f.get_runParameter(run, "AccountFilter")
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")
    groupTypes = f.get_runParameter(run, "groupTypes")

    if AccountFilter != None:
        account = entries[entries['Account'].str.contains(AccountFilter, na=False)]
    else:
        account = entries

    runningTotalFrame = pandas.get_crossJoinedFrames(pandas.get_uniqueFrame(account, "Account"),     pandas.get_uniqueFrame(entries, "Quantity_Type"))
    runningTotalFrame = runningTotalFrame.dropna(subset=["Account", "Quantity_Type"])


    change = entries.groupby( ["Account", "Quantity_Type"])["Quantity"].sum()
    result = runningTotalFrame.merge(change, how="left", on=["Account", "Quantity_Type"])
    result = result.fillna(0)
    result = pandas.get_cumulativesum(result)

    if fairValueCurrency != None:
        result["Price"] = result.apply(  lambda x: f.get_LatestPrice(PriceChanges, e["Date"].max(), x["Quantity_Type"], fairValueCurrency, 0, 5), axis=1)
        result["RunningTotal_FairValue"] =  result['RunningTotal'] * result['Price']
        result["Change_FairValue"] =  result['Quantity'] * result['Price']

        outputList = result[
            [ "Account", "Quantity_Type", "Quantity", "RunningTotal", "Price", "Change_FairValue", "RunningTotal_FairValue"]].rename(
            columns={
                "Quantity": "Change"
            })

        if groupTypes == "True":
            outputList = outputList[["Account", "Change_FairValue", "RunningTotal_FairValue"]]
            outputList = outputList.groupby(["Account"]).sum().reset_index()


    else:

        outputList = result[[ "Account", "Quantity_Type", "Quantity", "RunningTotal"]].rename(
        columns={
            "Quantity": "Change"
        } )

    pandas.write_file_balance(outputList,output,separator)


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
    groupTypes = f.get_runParameter(run, "groupTypes")


    e = pandas.read_file(input,separator )

    PriceChanges = f.get_priceUpdates(e)
    entries = f.get_transactions(e)


    if startDate == None:
        startDate = e["Date"].min()

    if endDate == None:
        endDate = e["Date"].max()

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




    if fairValueCurrency != None:
        UniquePriceCombo = pandas.get_crossJoinedFrames(pandas.get_dateFrame(startDate, endDate, increment), pandas.get_uniqueFrame(result, "Quantity_Type"))
        UniquePriceCombo["Price"] = UniquePriceCombo.apply( lambda x: f.get_LatestPrice(PriceChanges, x["Date"], x["Quantity_Type"], fairValueCurrency, 0, 5), axis=1)
        result = result.merge(UniquePriceCombo, on=["Date", "Quantity_Type"], how="inner")
        result["RunningTotal_FairValue"] = result.apply( lambda x: x["RunningTotal"] * x["Price"], axis=1)
        result["Change_FairValue"] = result.apply( lambda x: x["Quantity"] * x["Price"], axis=1)

        outputList = result[
            ["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal", "Price", "Change_FairValue", "RunningTotal_FairValue"]].rename(
            columns={
                "Quantity": "Change"
            })

        if groupTypes == "True":
            outputList = outputList[["Date", "Account", "Change_FairValue", "RunningTotal_FairValue"]]
            outputList = outputList.groupby(["Date","Account"]).sum().reset_index()


    else:

        outputList = result[["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal"]].rename(
        columns={
            "Quantity": "Change"
        } )

    pandas.write_file_balance(outputList,output,separator)

    pass

class Commands:

    def run_command(run, config):
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
            f.log("Other Command")
