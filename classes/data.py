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
    """
    Parses and processes entries based on the specified type by delegating to appropriate handlers.

    This function performs the following steps:
    1. Determines the type of data source from the `run` parameter.
    2. Calls the appropriate function to process entries based on the data source type.
    3. Handles different types of sources including IBKR, n26, wise, and yFinance.

    Parameters:
    - run (dict): Contains runtime parameters including the type of data source and other relevant details.
    - config (dict): Contains configuration settings that might be required by the data source handlers.

    """
    # Log the type of data source being processed
    data_type = run.get("type", "Unknown")
    f.log(f"Processing entries for data source type: {data_type}")

    # Process entries based on the specified type
    if data_type == "IBKR":
        f.log("Delegating to IBKR handler.")
        IBKR.write_Entries(run, config)
    elif data_type == "n26":
        f.log("Delegating to n26 handler.")
        n26.write_Entries(run, config)
    elif data_type == "wise":
        f.log("Delegating to wise handler.")
        wise.write_Entries(run, config)
    elif data_type == "yFinance":
        f.log("Delegating to yFinance handler.")
        yahooFinance.write_Entries(run, config)
    else:
        f.log(f"Unknown data source type: {data_type}. No handler found.")

    f.log("Parsing process completed.")


def command_merge(run, config):
    """
    Merges multiple CSV files into a single DataFrame, sorts by date, and writes the output to a file.

    This function performs the following steps:
    1. Retrieves the list of input file paths and the output file path from the provided parameters.
    2. Reads data from each input file and combines them into a single DataFrame.
    3. Sorts the combined DataFrame by the 'Date' column in ascending order.
    4. Writes the sorted DataFrame to an output file. If writing entries fails, attempts to write the balance data.

    Parameters:
    - run (dict): Contains runtime parameters including input file paths and output file path.
    - config (dict): Contains configuration settings such as CSV separator.

    """
    # Retrieve the input file paths and output file path
    inputs = f.get_runParameter(run, "inputs")
    output = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]

    # Initialize an empty list to hold DataFrames
    entries = []
    f.log("Starting the merge process.")

    for input in inputs:
        # Read data from each input file
        try:
            data = pandas.read_file(f.get_runParameter(input, "input"), separator)
            f.log(f"Successfully read data from {input}.")
        except Exception as e:
            f.log(f"Error reading data from {input}: {e}")
            continue  # Skip this file and continue with the next

        # Combine the data from all input files
        entries = f.combine_lists(entries, data)

    # Convert the list of DataFrames to a single DataFrame
    entries = pd.DataFrame(entries)
    f.log("Combined all data entries into a single DataFrame.")

    # Sort the DataFrame by the 'Date' column
    entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
    f.log("Sorted the DataFrame by 'Date' in ascending order.")

    # Write the sorted DataFrame to the output file
    try:
        pandas.write_file(entries, output, separator)
        f.log(f"Successfully wrote the merged data to {output}.")
    except Exception as e:
        f.log(f"Error writing merged data to {output}: {e}. Attempting to write balance data.")
        try:
            pandas.write_file(entries, output, separator)
            f.log(f"Successfully wrote balance data to {output}.")
        except Exception as e:
            f.log(f"Error writing balance data to {output}: {e}")

    f.log("Merge process completed.")


def command_filter(run, config):
    """
    Filters data based on provided filter rules and writes the filtered data to an output file.

    This function performs the following steps:
    1. Retrieves the input and output file paths and separator from the `run` and `config` parameters.
    2. Reads the data from the input file.
    3. Applies filter rules to the data.
    4. Writes the filtered data to the output file, handling any exceptions that may occur.

    Parameters:
    - run (dict): Contains runtime parameters including input file path, output file path, and filter rules.
    - config (dict): Contains configuration settings including CSV separator.
    """
    # Retrieve parameters from run and config
    input_path = f.get_runParameter(run, "input")
    output_path = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]

    # Log the input parameters
    f.log(f"Input file: {input_path}")
    f.log(f"Output file: {output_path}")
    f.log(f"CSV Separator: {separator}")

    # Read the data from the input file
    f.log("Reading data from input file.")
    data = pandas.read_file(input_path, separator)

    # Retrieve and apply filter rules to the data
    filters = f.get_runParameter(run, "filters")
    f.log(f"Applying filters: {filters}")
    data = f.run_filters(data, filters)

    # Attempt to write the filtered data to the output file
    try:
        f.log("Writing filtered data to output file.")
        pandas.write_file(data, output_path, separator)
    except Exception as e:
        f.log(f"Failed to write data using write_file_entries. Error: {e}")
        f.log("Attempting to write using write_file_balance.")
        try:
            pandas.write_file(data, output_path, separator)
        except Exception as e:
            f.log(f"Failed to write data using write_file_balance. Error: {e}")
            f.log("Unable to write filtered data to output file.")

    f.log("Data filtering and writing process completed.")


def command_validate(run, config):
    """
    Validates transactions to ensure they balance correctly and writes the results to an output file.

    This function performs the following steps:
    1. Retrieves input and output file paths and separator from the `run` and `config` parameters.
    2. Reads data from the input file.
    3. Filters the data to get transactions.
    4. For each unique transaction ID, checks if the transaction is valid (i.e., it balances out).
    5. Writes the validation results to the output file.

    Parameters:
    - run (dict): Contains runtime parameters including input file path and output file path.
    - config (dict): Contains configuration settings including CSV separator.
    """
    # Retrieve parameters from run and config
    input_path = f.get_runParameter(run, "input")
    output_path = f.get_runParameter(run, "output")
    separator = config["CSV_Separator"]

    # Log the input parameters
    f.log(f"Input file: {input_path}")
    f.log(f"Output file: {output_path}")
    f.log(f"CSV Separator: {separator}")

    # Read the data from the input file
    f.log("Reading data from input file.")
    data = pandas.read_file(input_path, separator)

    # Filter data to get only transactions
    f.log("Filtering data to get transactions.")
    data = f.filter_data(data, "Equals", "Type", "Transaction")

    # Initialize lists to store results
    ids = []
    results = []

    # Validate each unique transaction ID
    f.log("Validating transactions.")
    for transaction_ID in data["ID"].unique():
        # Filter data for the current transaction ID and validate
        transaction_data = f.filter_data(data, "Equals", "ID", transaction_ID)
        valid = pandas.validate_Transaction(transaction_data)

        # Append results
        ids.append(transaction_ID)
        results.append(valid)

    # Create a DataFrame to hold the validation results
    results_df = pd.DataFrame({
        "Transaction ID": ids,
        "Result": results
    })

    # Write the validation results to the output file
    f.log("Writing validation results to output file.")
    pandas.write_file(results_df, output_path, separator)

    f.log("Transaction validation process completed.")

def command_benchmark(run, config):
    """
    Adds benchmark entries based on a given benchmark ticker. For each transaction in the input data
    that matches specific criteria, the function calculates the benchmark quantity using the latest
    price of the benchmark ticker and appends a new 'Benchmark' entry to the data.

    The function performs the following steps:
    1. Reads input parameters (file paths, ticker, max depth, etc.).
    2. Filters the data for relevant transactions.
    3. For each matching transaction, calculates the benchmark quantity based on the ticker price.
    4. Adds a new benchmark entry and writes the updated data to the output file.

    Parameters:
    - run (dict): Contains runtime parameters including input/output file paths, benchmarkTicker, and benchmark account.
    - config (dict): Contains configuration settings such as the CSV separator.
    """

    # Step 1: Retrieve the initial parameters
    separator = config["CSV_Separator"]
    input_path = f.get_full_Path(run["input"])
    output_path = f.get_full_Path(run["output"])
    benchmark_ticker = f.get_runParameter(run, "benchmarkTicker")
    max_depth = f.get_runParameter(run, "maxDepth")

    # Log the retrieved parameters
    f.log(f"Input file: {input_path}")
    f.log(f"Output file: {output_path}")
    f.log(f"CSV Separator: {separator}")
    f.log(f"Benchmark Ticker: {benchmark_ticker}")
    f.log(f"Max Depth: {max_depth}")

    # Step 2: Read the input file
    f.log("Reading data from input file.")
    entries = pandas.read_file(input_path, separator)

    # Step 3: Filter entries to get only 'Transaction' types related to the benchmark account
    f.log("Filtering transactions related to the benchmark account.")
    entries_filtered = entries.loc[entries['Type'] == "Transaction"]
    entries_filtered = entries_filtered.loc[entries_filtered['Account'] == run["benchmark"]]
    entries_filtered = entries_filtered.loc[entries_filtered['ID'].str.contains("IBKR_")]

    f.log(f"Found {len(entries_filtered)} relevant transactions to process.")

    # Step 4: Iterate over each filtered transaction and calculate benchmark entries
    for index, row in entries_filtered.iterrows():
        date = row["Date"]

        # Get price updates and the latest price for the benchmark ticker
        price_changes = f.get_priceUpdates(entries)
        price = f.get_LatestPrice(price_changes, row["Date"], benchmark_ticker, row["Quantity_Type"], 0, max_depth)

        # Calculate the benchmark quantity (if no price is found, set quantity to 0)
        if price is None:
            f.log(f"No price found for {benchmark_ticker} on {date}, setting quantity to 0.")
            quantity = 0
        else:
            quantity = round(Decimal(row["Quantity"]) / Decimal(price))
            f.log(f"Price found for {benchmark_ticker} on {date}: {price}, calculated quantity: {quantity}.")

        # Step 5: Create the benchmark entry
        benchmark_entry = pd.DataFrame([{
            "Date": date,
            "Type": "Benchmark",
            "ID": "Benchmark_" + str(random.randrange(0, 99999999999999)),
            "Name": benchmark_ticker,
            "Account": "Benchmark",
            "Quantity": Decimal(quantity).copy_negate(),  # Set the benchmark quantity as negative
            "Quantity_Type": benchmark_ticker,
            "Cost": Decimal(row["Quantity"]).copy_negate(),  # Set the cost as negative
            "Cost_Type": row["Quantity_Type"],
        }])

        f.log(f"Generated benchmark entry for {benchmark_ticker} on {date}: {benchmark_entry.to_dict()}")

        # Append the benchmark entry to the main entries data
        entries = pd.concat([entries, benchmark_entry], ignore_index=True)

    # Step 6: Write the updated entries to the output file
    f.log(f"Writing updated data to {output_path}.")
    pandas.write_file(entries, output_path, separator)

    f.log("Benchmark processing completed successfully.")
    pass


def command_balance(run, config):
    """
    Calculates account balances and optionally computes fair value if a currency and date are provided.

    This function performs the following steps:
    1. Retrieves input/output paths, separator, and relevant parameters such as fairValueCurrency, fairValueDate, and groupTypes.
    2. Reads the input data and filters it.
    3. Cross joins accounts and quantity types to compute the total balance for each combination.
    4. If a fair value currency is provided, computes the fair value for each asset based on the latest prices.
    5. Writes the results (balance or fair value) to an output file.

    Parameters:
    - run (dict): Contains runtime parameters including input file path and output file path.
    - config (dict): Contains configuration settings such as the CSV separator.
    """

    # Step 1: Retrieve initial parameters
    separator = config["CSV_Separator"]
    input_path = f.get_full_Path(run["input"])
    output_path = f.get_full_Path(run["output"])
    fairValueCurrency = f.get_runParameter(run, "fairValueCurrency")
    fairValueDate = f.get_runParameter(run, "fairValueDate")
    groupTypes = f.get_runParameter(run, "groupTypes")

    # Log initial parameters
    f.log(f"Input file: {input_path}")
    f.log(f"Output file: {output_path}")
    f.log(f"CSV Separator: {separator}")
    f.log(f"Fair Value Currency: {fairValueCurrency}")
    f.log(f"Fair Value Date: {fairValueDate}")
    f.log(f"Group Types: {groupTypes}")

    # Step 2: Read the input data
    f.log("Reading data from input file.")
    data = pandas.read_file(input_path, separator)

    # Step 3: Get price updates and filter by fair value date if provided
    f.log("Fetching price updates.")
    price_changes = f.get_priceUpdates(data)
    if fairValueDate:
        try:
            price_changes = f.filter_data(price_changes, "Max", "Date", fairValueDate)
            f.log(f"Filtered price changes up to {fairValueDate}.")
        except Exception as e:
            f.log(f"No fair value date or error encountered: {str(e)}")

    # Step 4: Filter transactions based on the rules
    filters = f.get_runParameter(run, "filters")
    f.log(f"Applying filters: {filters}")
    filtered_data = f.run_filters(data, filters)

    # Step 5: Cross join accounts and quantity types to create the balance frame
    f.log("Generating cross-joined frames for accounts and quantity types.")
    balance_frame = pandas.get_crossJoinedFrames(
        pandas.get_uniqueFrame(filtered_data, "Account"),
        pandas.get_uniqueFrame(filtered_data, "Quantity_Type")
    )

    # Remove rows with missing Account or Quantity_Type
    balance_frame = balance_frame.dropna(subset=["Account", "Quantity_Type"])

    # Step 6: Group by Account and Quantity_Type to sum up the quantities
    f.log("Calculating total quantities for each Account and Quantity_Type.")
    changes = filtered_data.groupby(["Account", "Quantity_Type"])["Quantity"].sum()

    # Merge the sum into the balance frame and fill missing values with 0
    result = balance_frame.merge(changes, how="left", on=["Account", "Quantity_Type"]).fillna(0)

    # Step 7: If fair value currency is provided, compute fair value
    if fairValueCurrency:
        f.log(f"Calculating fair value using currency {fairValueCurrency}.")

        # Fetch the latest prices and calculate fair value
        result["Price"] = result.apply(
            lambda x: f.get_LatestPrice(price_changes, fairValueDate, x["Quantity_Type"], fairValueCurrency, 0, 5),
            axis=1
        )
        result["Change_FairValue"] = result["Quantity"] * result["Price"]

        # Prepare output data with fair value columns
        output_list = result[["Account", "Quantity_Type", "Quantity", "Price", "Change_FairValue"]].rename(
            columns={"Quantity": "Change"})

        # Round values to 4 decimal places for better readability
        output_list = output_list.round({'Change': 4, 'Change_FairValue': 4, 'Price': 4})

        # Step 8: Group types into a single one if required
        if groupTypes:
            f.log("Grouping types into a single one.")
            output_list = output_list[["Account", "Change_FairValue"]].groupby("Account").sum().reset_index()

    # Step 9: If no fair value is required, just return the balance
    else:
        f.log("No fair value currency provided, calculating raw balance.")
        output_list = result[["Account", "Quantity_Type", "Quantity"]].rename(columns={"Quantity": "Change"})

        # Remove rows with zero balances
        output_list = output_list[output_list["Change"] != 0].round({'Change': 4})

    # Step 10: Write the output to a file
    f.log(f"Writing the output to {output_path}.")
    pandas.write_file(output_list, output_path, separator)

    f.log("Balance calculation completed successfully.")
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
    pandas.write_file(outputList, output_path, separator)

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
    pandas.write_file(output_Entries_reset, output, separator)
    f.log("Compression complete and data written to file.")