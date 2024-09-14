import random

import pandas as pd
from decimal import Decimal
from classes.functions import Functions as f
import classes.pandas as pandas
import numpy as np  # Importing numpy for NaN

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
    # We write the output to a file
    try:
        pandas.write_file_entries(entries, output, separator)
    except:
        pandas.write_file_balance(entries, output, separator)
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
    try:
        pandas.write_file_entries(data, output, separator)
    except:
        pandas.write_file_balance(data, output, separator)
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
    try:
        PriceChanges = f.filter_data(PriceChanges, "Max", "Date", fairValueDate)
    except:
        f.log("No fair value date")
    # We get only the transactions
    #data = f.get_transactions(e)
    data = e
    # We get the filter rules and adjust our input data accordingly
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data,filters )


    # We get the cross join frame
    runningTotalFrame = pandas.get_crossJoinedFrames(pandas.get_uniqueFrame(data, "Account"), pandas.get_uniqueFrame(data, "Quantity_Type"))
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
        outputList.round({'Change': 4})
        outputList.round({'Change_FairValue': 4})
        outputList.round({'Price': 4})

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
        # We remove the
        outputList = outputList[~(outputList['Change'] == 0.0)]
        outputList.round({'Change': 4})


    # We write the output to a file

    pandas.write_file_balance(outputList,output,separator)


    pass



def command_runningTotal(run, config):
    """
    Generates a running total report based on transaction and benchmark data.

    This function performs the following steps:
    1. Retrieves configuration settings and parameters such as file paths, increment frequency, fair value currency, and grouping options.
    2. Reads input data from a CSV file and extracts relevant transaction and benchmark data.
    3. Merges the transaction and benchmark data into a single DataFrame.
    4. Applies any specified filters to the data.
    5. Constructs a DataFrame to hold running totals by performing a cross join of dates, accounts, and quantity types.
    6. Calculates the quantity changes and running totals, then computes cumulative sums.
    7. Optionally, if a fair value currency is specified, calculates the fair value of the running totals and changes based on up-to-date prices.
    8. Produces a final output DataFrame, which may aggregate results based on the groupTypes parameter.
    9. Writes the resulting DataFrame to a CSV file.

    Parameters:
    - run (dict): Contains runtime parameters including input and output file paths, and other configuration settings.
    - config (dict): Contains configuration settings such as CSV separator.

    Notes:
    - The function uses `pandas` for data manipulation and `f` (assumed to be a utility module) for additional data processing tasks.
    - The fair value calculation is conditional based on whether a valid currency is provided.
    - Results can be grouped by account if `groupTypes` is set to True.

    """

    # Extract configuration parameters
    separator = config["CSV_Separator"]
    input_path = f.get_full_Path(run["input"])
    output_path = f.get_full_Path(run["output"])
    increment = f.get_runParameter(run, "increment")
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")
    groupTypes = f.get_runParameter(run, "groupTypes")

    # Log the start of the process
    f.log(f"Starting running total calculation with parameters: separator={separator}, input={input_path}, output={output_path}, increment={increment}, fairValueCurrency={fairValueCurrency}, groupTypes={groupTypes}")

    # Read the data
    data = pandas.read_file(input_path, separator)

    # Extract price updates and transactions
    PriceChanges = f.get_priceUpdates(data)
    transactions = f.get_transactions(data)
    benchmark = f.get_benchmark(data)

    # Combine transactions and benchmark into a single DataFrame
    data_combined = pd.concat([transactions, benchmark], ignore_index=True)

    # Apply filters to the data
    filters = f.get_runParameter(run, "filters")
    data_filtered = f.run_filters(data_combined, filters)

    # Define the start and end dates
    startDate = data_filtered["Date"].min()
    endDate = data_filtered["Date"].max()

    # Generate the date, account, and quantity_type combinations for the running total
    runningTotalFrame = pandas.get_crossJoinedFrames(
        pandas.get_dateFrame(startDate, endDate, increment),
        pandas.get_uniqueFrame(data_filtered, "Account")
    )
    runningTotalFrame = pandas.get_crossJoinedFrames(
        runningTotalFrame,
        pandas.get_uniqueFrame(data_filtered, "Quantity_Type")
    )

    # Drop rows with missing required information
    runningTotalFrame = runningTotalFrame.dropna(subset=["Account", "Date", "Quantity_Type"])

    # Calculate changes and cumulative sums
    change = data_filtered.groupby(by=[pd.Grouper(key='Date', freq=increment), "Account", "Quantity_Type"])["Quantity"].sum()
    result = runningTotalFrame.merge(change, how="left", on=["Date", "Account", "Quantity_Type"])
    result = result.fillna(0)
    result = pandas.get_cumulativesum(result)

    # Check if fair value calculation is required
    if fairValueCurrency:  # fairValueCurrency should be either a string or None
        # Generate price table
        UniquePriceCombo = pandas.get_crossJoinedFrames(
            pandas.get_dateFrame(startDate, endDate, increment),
            pandas.get_uniqueFrame(result, "Quantity_Type")
        )
        UniquePriceCombo["Price"] = UniquePriceCombo.apply(
            lambda x: f.get_LatestPrice(PriceChanges, x["Date"], x["Quantity_Type"], fairValueCurrency, 0, 5),
            axis=1
        )

        # Merge price table with the result and compute fair values
        result = result.merge(UniquePriceCombo, on=["Date", "Quantity_Type"], how="inner")
        result["RunningTotal_FairValue"] = result["RunningTotal"] * result["Price"]
        result["Change_FairValue"] = result["Quantity"] * result["Price"]

        # Prepare output with fair value
        outputList = result[
            ["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal", "Price", "Change_FairValue", "RunningTotal_FairValue"]
        ].rename(columns={"Quantity": "Change"})

        # Group types if required
        if groupTypes:
            outputList = outputList[["Date", "Account", "Change_FairValue", "RunningTotal_FairValue"]]
            outputList = outputList.groupby(["Date", "Account"]).sum().reset_index()

    else:
        # Prepare output without fair value
        outputList = result[
            ["Date", "Account", "Quantity_Type", "Quantity", "RunningTotal"]
        ].rename(columns={"Quantity": "Change"})

    # Write the output to a file
    pandas.write_file_balance(outputList, output_path, separator)

    # Log the completion
    f.log("Running total calculation completed and output written to file.")

    pass


def command_chart(run, config):
    """
    Generates charts based on the configuration and input data.

    Parameters:
    run (dict): Contains 'input' and 'output' paths and other parameters for the chart.
    config (dict): Configuration containing 'CSV_Separator'.
    """

    # Retrieve configuration parameters
    separator = config.get("CSV_Separator", ",")
    input = f.get_full_Path(run.get("input"))
    output = f.get_full_Path(run.get("output"))
    chart_params = {
        "type": f.get_runParameter(run, "type"),
        "index_Name": f.get_runParameter(run, "index_Name"),
        "column_Name": f.get_runParameter(run, "column_Name"),
        "value_Name": f.get_runParameter(run, "value_Name"),
        "colormap": f.get_runParameter(run, "colormap"),
        "title": f.get_runParameter(run, "title"),
        "invert": f.get_runParameter(run, "invert"),
        "max_legend_entries": f.get_runParameter(run, "max_legend_entries"),
        "rounding": f.get_runParameter(run, "rounding")
    }

    # Read the input data
    data = pandas.read_file(input, separator)
    f.log(f"Input data loaded from {input}.")

    # Apply filters to the data
    filters = f.get_runParameter(run, "filters")
    data = f.run_filters(data, filters)
    f.log(f"Applied filters: {filters}.")

    # Invert values if required
    if chart_params["invert"]:
        data[chart_params["value_Name"]] = -data[chart_params["value_Name"]]
        f.log(f"Inverted values in column {chart_params['value_Name']}.")

    # Import the charting class
    import classes.charts as charts
    f.log("Charting class imported.")

    # Define chart type to function mapping
    chart_functions = {
        "stackedBar": charts.generate_stackedBarChart,
        "Bar": charts.generate_BarChart,
        "pieChart": charts.generate_pieChart,
        "stackedlineChart": charts.generate_stackedlineChart,
        "lineChart": charts.generate_lineChart
    }

    # Generate the chart based on the specified type
    chart_func = chart_functions.get(chart_params["type"])
    if chart_func:
        chart_func(data, chart_params["index_Name"], chart_params["column_Name"], chart_params["value_Name"],
                   output, chart_params["title"], chart_params["colormap"], chart_params["max_legend_entries"],
                   chart_params["rounding"])
        f.log(f"Chart of type {chart_params['type']} generated and saved to {output}.")
    else:
        f.log(f"Invalid chart type specified: {chart_params['type']}")

    pass


def command_compress(run, config):
    """
    Compresses transaction data into a summary DataFrame and writes it to a file.

    Parameters:
    run (dict): Contains 'input' and 'output' paths.
    config (dict): Configuration containing 'CSV_Separator'.
    """

    # Retrieve separator and file paths from the configuration
    separator = config["CSV_Separator"]
    input = f.get_full_Path(run["input"])
    output = f.get_full_Path(run["output"])

    # Read transaction files
    input_data = pandas.read_file(input, separator)

    # Extract different types of entries
    transactions = f.get_transactions(input_data)

    # Get unique values for multi-index creation
    unique_Account = transactions["Account"].unique()
    unique_Quantity_Type = transactions["Quantity_Type"].unique()
    unique_Cost_Type = transactions["Cost_Type"].unique()

    # Create MultiIndex DataFrame
    keys = [unique_Account, unique_Quantity_Type, unique_Cost_Type]
    output_Entries = pd.MultiIndex.from_product(keys, names=["Account", "Quantity_Type", "Cost_Type"]).to_frame()

    # Initialize 'Quantity' and 'Cost' columns with Decimal values
    output_Entries['Quantity'] = Decimal(0.0)
    output_Entries['Cost'] = Decimal(0.0)

    # Sort the DataFrame by MultiIndex to optimize performance
    output_Entries = output_Entries.sort_index()
    f.log("Initialized MultiIndex DataFrame and sorted by index.")

    # Update the 'Quantity' and 'Cost' columns
    for index, row in transactions.iterrows():
        account = row["Account"]
        quantity_type = row["Quantity_Type"]
        cost_type = row["Cost_Type"]

        new_quantity = row['Quantity'] if pd.notna(row['Quantity']) else Decimal(0.0)
        new_cost = row['Cost'] if pd.notna(row['Cost']) else Decimal(0.0)

        # Use .at for scalar updates which can be more efficient
        loc_index = (account, quantity_type, cost_type)
        if loc_index in output_Entries.index:
            current_quantity = output_Entries.at[loc_index, 'Quantity']
            current_cost = output_Entries.at[loc_index, 'Cost']

            # Update values
            output_Entries.at[loc_index, 'Quantity'] = current_quantity + new_quantity
            output_Entries.at[loc_index, 'Cost'] = current_cost + new_cost
        else:
            # If the index is not present, you might need to handle this case
            f.log(f"Index {loc_index} not found in output_Entries.")

    # Drop rows where both 'Quantity' and 'Cost' are NaN
    output_Entries = output_Entries.dropna(subset=['Quantity', 'Cost'], how='all')

    # Reset index to convert MultiIndex to columns
    output_Entries_reset = output_Entries.reset_index(drop=True)

    # Drop rows where both 'Quantity' and 'Cost' are 0.0
    output_Entries_reset = output_Entries_reset[
        ~((output_Entries_reset['Quantity'] == Decimal(0.0)) & (output_Entries_reset['Cost'] == Decimal(0.0)))
    ]

    # Add additional columns
    output_Entries_reset["Date"] = max(input_data["Date"])
    output_Entries_reset["Type"] = "Transaction"
    output_Entries_reset["ID"] = "Compress_" + str(random.randrange(0, 99999999999999))
    output_Entries_reset["Name"] = "End of period compression"

    # Write the result to a file
    pandas.write_file_entries(output_Entries_reset, output, separator)
    f.log("Compression complete and data written to file.")