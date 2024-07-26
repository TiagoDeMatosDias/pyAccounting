import random

import pandas as pd
from decimal import Decimal
from classes.functions import Functions as f
import classes.pandas as pandas

import classes.parser_IBKR as IBKR
import classes.parser_n26 as n26
import classes.parser_wise as wise
import classes.parser_yahooFinance as yahooFinance


def command_parser(run, config):
    if run["type"] == "IBKR":
        IBKR.write_Entries(run, config)
        pass
    elif run["type"] == "n26":
        n26.write_Entries(run, config)
        pass
    elif run["type"] == "wise":
        wise.write_Entries(run, config)
        pass
    elif run["type"] == "yFinance":
        yahooFinance.write_Entries(run,config)
        pass
    pass


def command_merge(run, config):
    # We get the initial parameters, including the separator and the input and output location
    inputs = f.get_runParameter(run, "inputs")
    output = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]

    # We combine the data
    entries = []
    for input in inputs:
        # We get the data
        data = pandas.read_file(f.get_runParameter(input, "input"), separator)
        entries = f.combine_lists(entries, data)

    # We sort it by Date
    entries = pd.DataFrame(entries)
    entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)

    # We write the output to a file
    pandas.write_file_entries(entries, output, separator)
    pass


def command_filter(run, config):
    # We get the initial parameters, including the separator and the input and output location
    input = f.get_runParameter(run, "input")
    output = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]

    # We get the data
    data = pandas.read_file(input, separator)

    # We get the filter rules and adjust our input data accordingly
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data, filters)

    # We write the output to a file
    pandas.write_file_entries(data, output, separator)

def command_validate(run,config):
    # We get the initial parameters, including the separator and the input and output location
    input = f.get_runParameter(run, "input")
    output = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]


    # We get the data
    data = pandas.read_file(input, separator)

    # We get only the transactions
    transactions = f.filter_data(data, "Equals", "Type", "Transaction")

    # for every unique transaction, we check if it balances out

    id=[]
    result=[]
    for transaction_ID in data["ID"].unique():
        Valid = pandas.validate_Transaction(f.filter_data(data, "Equals", "ID", transaction_ID))
        id.append(transaction_ID)
        result.append(Valid)

    results = {
        "Transaction ID": id,
        "Result": result,
    }

    results = pd.DataFrame(results)

    pandas.write_file_balance(results, output, separator)

    pass


def command_benchmark(run, config):
    # We get the initial parameters, including the separator and the input and output location
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    benchmarkTicker = f.get_runParameter(run, "benchmarkTicker")
    maxDepth = f.get_runParameter(run, "maxDepth")

    # We read the file
    entries = pandas.read_file(input,separator )


    entries_filtered = entries.loc[entries['Type'] == "Transaction"]
    entries_filtered = entries_filtered.loc[entries_filtered['Account'] == run["benchmark"]]
    entries_filtered = entries_filtered.loc[entries_filtered['ID'].str.contains("IBKR_")]

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
            "ID": "Benchmark_" +  str(random.randrange(0,99999999999999)),
            "Name": benchmarkTicker,
            "Account": "Benchmark",
            "Quantity": Decimal( quantity).copy_negate(),
            "Quantity_Type": benchmarkTicker,
            "Cost": Decimal( row["Quantity"]).copy_negate(),
            "Cost_Type": row["Quantity_Type"],
        }])

        entries = pd.concat([entries, benchmarkEntry], ignore_index=True)

    # We write the output to a file
    pandas.write_file_entries(entries,output,separator)

    pass





def command_balance(run, config):

    # We get the initial parameters, including the separator and the input and output location
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")
    fairValueDate = f.get_runParameter(run, "fairValueDate")
    groupTypes = f.get_runParameter(run, "groupTypes")

    # We read the data
    e = pandas.read_file(input,separator )

    # We get only the price updates up to the fair value date
    PriceChanges = f.get_priceUpdates(e)
    PriceChanges = f.filter_data(PriceChanges, "Max", "Date", fairValueDate)

    # We get only the transactions
    #data = f.get_transactions(e)
    data = e
    # We get the filter rules and adjust our input data accordingly
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data,filters )


    # We get the cross join frame
    runningTotalFrame = pandas.get_crossJoinedFrames(pandas.get_uniqueFrame(data, "Account"),     pandas.get_uniqueFrame(data, "Quantity_Type"))
    runningTotalFrame = runningTotalFrame.dropna(subset=["Account", "Quantity_Type"])

    # We get the data for the frame and do the sum
    change = data.groupby( ["Account", "Quantity_Type"])["Quantity"].sum()

    # We merge the sums with the cross join, filling out the empty values
    result = runningTotalFrame.merge(change, how="left", on=["Account", "Quantity_Type"])
    result = result.fillna(0)

    # If we have a fair value currency, we use it to get the fair value of the balance at the last date
    if fairValueCurrency != None:
        # We set the price of the assets
        result["Price"] = result.apply(  lambda x: f.get_LatestPrice(PriceChanges, fairValueDate, x["Quantity_Type"], fairValueCurrency, 0, 5), axis=1)

        # We generate the Fair Value columns based on the Price
        result["Change_FairValue"] =  result['Quantity'] * result['Price']

        # We generate the output
        outputList = result[
            [ "Account", "Quantity_Type", "Quantity",  "Price", "Change_FairValue"]].rename(
            columns={
                "Quantity": "Change"
            })

        #We determine if we want to group the types into a single one
        if groupTypes:
            outputList = outputList[["Account", "Change_FairValue"]]
            outputList = outputList.groupby(["Account"]).sum().reset_index()

    #If we don't want the fair value then we get just the balance
    else:
        outputList = result[[ "Account", "Quantity_Type", "Quantity"]].rename(
        columns={
            "Quantity": "Change"
        } )

    # We write the output to a file
    pandas.write_file_balance(outputList,output,separator)


    pass




def command_runningTotal(run, config):
    # We get the initial parameters, including the separator and the input and output location
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    increment = f.get_runParameter(run, "increment")
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")
    groupTypes = f.get_runParameter(run, "groupTypes")

    # We read the data
    e = pandas.read_file(input,separator )

    # We get only the price updates
    PriceChanges = f.get_priceUpdates(e)

    # We get only the transactions
    #data = f.get_transactions(e)
    data = e

    # We get the filter rules and adjust our input data accordingly
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data,filters )

    # We define the start and end date
    startDate = data["Date"].min()
    endDate = data["Date"].max()

    # We cross join the frames to get the Date, Account, Quantity_Type index
    runningTotalFrame = pandas.get_crossJoinedFrames(pandas.get_dateFrame(startDate, endDate, increment),
                                                     pandas.get_uniqueFrame(data, "Account"))

    runningTotalFrame = pandas.get_crossJoinedFrames(runningTotalFrame,
                                                     pandas.get_uniqueFrame(data, "Quantity_Type"))

    # We drop the empty subsets
    runningTotalFrame = runningTotalFrame.dropna(subset=["Account", "Date", "Quantity_Type"])

    # We get the changes and cumulative sum
    change = data.groupby(by=[pd.Grouper(key='Date', freq=increment), "Account", "Quantity_Type"])["Quantity"].sum()
    result = runningTotalFrame.merge(change, how="left", on=["Date", "Account", "Quantity_Type"])
    result = result.fillna(0)
    result = pandas.get_cumulativesum(result)



    # We check if we want the fair value
    if fairValueCurrency != None:
        # We generate a table with the prices by period
        UniquePriceCombo = pandas.get_crossJoinedFrames(pandas.get_dateFrame(startDate, endDate, increment), pandas.get_uniqueFrame(result, "Quantity_Type"))
        UniquePriceCombo["Price"] = UniquePriceCombo.apply( lambda x: f.get_LatestPrice(PriceChanges, x["Date"], x["Quantity_Type"], fairValueCurrency, 0, 5), axis=1)

        # We merge the price table into the overall table, and multiply the quantity by the price to get the fair value
        result = result.merge(UniquePriceCombo, on=["Date", "Quantity_Type"], how="inner")
        result["RunningTotal_FairValue"] = result.apply( lambda x: x["RunningTotal"] * x["Price"], axis=1)
        result["Change_FairValue"] = result.apply( lambda x: x["Quantity"] * x["Price"], axis=1)

        # We generate the output
        outputList = result[
            ["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal", "Price", "Change_FairValue", "RunningTotal_FairValue"]].rename(
            columns={
                "Quantity": "Change"
            })

        #We determine if we want to group the types into a single one
        if groupTypes == True:
            outputList = outputList[["Date", "Account", "Change_FairValue", "RunningTotal_FairValue"]]
            outputList = outputList.groupby(["Date","Account"]).sum().reset_index()

    #If we don't want the fair value then we get just the change
    else:

        outputList = result[["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal"]].rename(
        columns={
            "Quantity": "Change"
        } )

    # We write the output to a file
    pandas.write_file_balance(outputList,output,separator)

    pass




def command_chart(run, config):

    # We get the initial parameters, including the separator and the input and output location
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])
    type = f.get_runParameter(run, "type")
    index_Name = f.get_runParameter(run, "index_Name")
    column_Name = f.get_runParameter(run, "column_Name")
    value_Name = f.get_runParameter(run, "value_Name")
    colormap = f.get_runParameter(run, "colormap")
    title = f.get_runParameter(run, "title")
    invert = f.get_runParameter(run, "invert")
    max_legend_entries = f.get_runParameter(run, "max_legend_entries")
    rounding  = f.get_runParameter(run, "rounding")

    # We get the input data
    data = pandas.read_file(input,separator )

    # We get the filter rules and adjust our input data accordingly
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data,filters )

    # We invert the values if required
    if invert:
        data[value_Name] = -data[value_Name]

    # We import the charting class
    import classes.charts as charts

    # we call the relevant charting function
    if type == "stackedBar":
        charts.generate_stackedBarChart(data,index_Name, column_Name, value_Name,output,title, colormap , max_legend_entries, rounding)
    if type == "Bar":
        charts.generate_BarChart(data,index_Name, column_Name, value_Name,output,title, colormap , max_legend_entries, rounding)
    if type == "pieChart":
        charts.generate_pieChart(data, column_Name, value_Name, output,title, colormap )
    if type == "stackedlineChart":
        charts.generate_stackedlineChart(data,index_Name, column_Name, value_Name,output,title, colormap , max_legend_entries, rounding)
    if type == "lineChart":
        charts.generate_lineChart(data,index_Name, column_Name, value_Name,output,title, colormap , max_legend_entries, rounding)

    pass
