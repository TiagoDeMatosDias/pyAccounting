import math
import random
import numpy as np
from classes.functions import Functions as functions
from decimal import Decimal
import pandas as pd
import classes.pandas as pandas


def write_Entries(run, config):
    """
    Writes transaction entries to a CSV file based on the configuration for N26 transactions.

    Parameters:
    - run (dict): Contains runtime parameters like the output file path.
    - config (dict): Contains the N26 parser configuration (e.g., CSV separator, config location).
    """
    output = functions.get_runParameter(run, "output")
    functions.log(f"Writing N26 entries to {output}.")

    # Import entries based on the N26 configuration
    entries = import_Entries(config["Config_n26"])

    # Write the entries to the specified output file
    pandas.write_file_entries(entries, output, config["CSV_Separator"])
    functions.log(f"Entries successfully written to {output}.")
    pass


def import_Entries(parserLocation):
    """
    Imports transaction entries from the input files specified in the N26 parser configuration.

    Parameters:
    - parserLocation (str): Path to the N26 parser configuration.

    Returns:
    - pd.DataFrame: A DataFrame containing all the parsed transaction entries.
    """
    functions.log(f"Importing entries from {parserLocation}.")

    # Load parser configuration and get input file list
    parser_config = functions.import_json(parserLocation)
    input_folder = functions.get_full_Path(parser_config["input"])
    input_files = functions.get_ListFilesInDir(input_folder)

    # Try to read the rules table, if available
    try:
        rules = pd.read_csv(filepath_or_buffer=parser_config['RulesTable'], sep=parser_config['RulesTable_Separator'])
        functions.log("Rules table successfully loaded.")
    except:
        functions.log(f"No rules available at {parser_config['RulesTable']}. Returning empty DataFrame.")
        return pd.DataFrame()

    # Process each input file and combine entries
    entries = []
    for input_file in input_files:
        entries = functions.combine_lists(get_entriesFromFile(input_file, parser_config, rules), entries)

    entries = pd.DataFrame(entries)

    # Sort entries by date
    try:
        entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
        functions.log("Entries sorted by Date.")
    except:
        functions.log("Unable to sort entries by Date.")

    return entries


def get_entriesFromFile(inputFile, parser_config, rules):
    """
    Extracts entries from a given input file based on its format.

    Parameters:
    - inputFile (str): Path to the input file.
    - parser_config (dict): Configuration for parsing the file.
    - rules (pd.DataFrame): DataFrame containing account mapping rules.

    Returns:
    - list: A list of entries extracted from the file.
    """
    functions.log(f"Processing entries from file {inputFile}.")
    entries = []

    # Determine file format (1 or 2)
    file_format = get_File_Format(inputFile)

    if file_format == 1:
        functions.log(f"File format 1 detected for {inputFile}.")
        n26 = pd.read_csv(filepath_or_buffer=inputFile, sep=parser_config["separator"], parse_dates=["Date"],
                          date_format=parser_config["DateFormat"])
        for _, row in n26.iterrows():
            entries = functions.combine_lists(convert_transaction_format_1(row, parser_config, rules), entries)
    elif file_format == 2:
        functions.log(f"File format 2 detected for {inputFile}.")
        n26 = pd.read_csv(filepath_or_buffer=inputFile, sep=parser_config["separator"], parse_dates=["Booking Date"],
                          date_format=parser_config["DateFormat"])
        for _, row in n26.iterrows():
            entry = convert_transaction_format_2(row, parser_config, rules)
            if entry is not None:
                entries = functions.combine_lists(entry, entries)

    return entries


def convert_transaction_format_1(row, parser_config, rules):
    """
    Converts a row of N26 data from format 1 into a structured entry.

    Parameters:
    - row (pd.Series): A row of transaction data.
    - parser_config (dict): Configuration for parsing.
    - rules (pd.DataFrame): Account mapping rules.

    Returns:
    - dict: A dictionary representing the structured transaction entry.
    """
    Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type = ([] for i in range(9))

    account_subAccount = parser_config["SubAccounts"]
    name = str(str(row["Payee"]) + "_" + str(row["Account number"]) + str(row["Payment reference"])).replace(" ","").replace(",","").upper()

    # Determine account based on transaction type
    if row["Transaction type"] == "Outgoing Transfer" and str(row["Account number"]) == "nan" and row[
        "Payee"] != "N26 Bank":
        account = "Assets:Banks:n26:Spaces"
    elif row["Transaction type"] == "Income" and str(row["Account number"]) == "nan":
        account = "Assets:Banks:n26:Spaces"
    else:
        account = get_Account(name, parser_config, rules)

    id = f"N26_{random.randrange(0, 99999999999999)}"

    # Append two entries for the transaction
    Date.extend([row["Date"], row["Date"]])
    Type.extend(["Transaction", "Transaction"])
    ID.extend([id, id])
    Name.extend([name, name])
    Account.extend([account_subAccount, account])
    Quantity.extend([row["Amount (EUR)"], Decimal(row["Amount (EUR)"]).copy_negate()])
    Quantity_Type.extend([parser_config['DefaultCurrency'], parser_config['DefaultCurrency']])
    Cost.extend([None, None])
    Cost_Type.extend([None, None])

    # Combine data into a dictionary
    return {
        "Date": Date,
        "Type": Type,
        "ID": ID,
        "Name": Name,
        "Account": Account,
        "Quantity": Quantity,
        "Quantity_Type": Quantity_Type,
        "Cost": Cost,
        "Cost_Type": Cost_Type,
    }


def convert_transaction_format_2(row, parser_config, rules):
    """
    Converts a row of N26 data from format 2 into a structured entry.

    Parameters:
    - row (pd.Series): A row of transaction data.
    - parser_config (dict): Configuration for parsing.
    - rules (pd.DataFrame): Account mapping rules.

    Returns:
    - dict or None: A dictionary representing the structured transaction entry or None if invalid.
    """
    Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type = ([] for i in range(9))

    account_subAccount = parser_config["SubAccounts"]
    name = str(str(row["Partner Name"]) + "_" + str(row["Partner Iban"]) + str(row["Payment Reference"])).replace(" ","").replace(",","").upper()

    # Ignore rows that are not part of the main account
    if row["Account Name"] != "Main Account":
        return None

    # Determine account based on transaction type
    if row["Account Name"] == "Main Account" and str(row["Partner Iban"]) == "nan" and (
            str(row["Type"]) in ["Credit Transfer", "Debit Transfer"]):
        account = "Assets:Banks:n26:Spaces"
    else:
        account = get_Account(name, parser_config, rules)

    id = f"N26_{random.randrange(0, 99999999999999)}"

    # Append two entries for the transaction
    Date.extend([row["Booking Date"], row["Booking Date"]])
    Type.extend(["Transaction", "Transaction"])
    ID.extend([id, id])
    Name.extend([name, name])
    Account.extend([account_subAccount, account])
    Quantity.extend([row["Amount (EUR)"], Decimal(row["Amount (EUR)"]).copy_negate()])
    Quantity_Type.extend([parser_config['DefaultCurrency'], parser_config['DefaultCurrency']])
    Cost.extend([None, None])
    Cost_Type.extend([None, None])

    # Combine data into a dictionary
    return {
        "Date": Date,
        "Type": Type,
        "ID": ID,
        "Name": Name,
        "Account": Account,
        "Quantity": Quantity,
        "Quantity_Type": Quantity_Type,
        "Cost": Cost,
        "Cost_Type": Cost_Type,
    }


def get_Account(name, parser_config, rules):
    """
    Maps the name to an account using the rules table.

    Parameters:
    - name (str): The concatenated name used to search for the account.
    - parser_config (dict): Configuration for parsing.
    - rules (pd.DataFrame): Account mapping rules.

    Returns:
    - str: The account name.
    """
    default_account = parser_config['UndefinedAccount']

    for _, row in rules.iterrows():
        if row["Source"].upper() in name:
            if row["Exact"] == True:
                if row["Source"].upper() == name:
                    return row["Account"]
            elif not row["Exact"]:
                return row["Account"]

    return default_account


def get_File_Format(inputFile):
    """
    Determines the format of the file based on its header.

    Parameters:
    - inputFile (str): Path to the input file.

    Returns:
    - int: The format of the file (1, 2, or 0 if unknown).
    """
    with open(inputFile) as f:
        first_line = f.readline()

    # File format detection based on known headers
    format_2023 = '"Date","Payee","Account number","Transaction type","Payment reference","Amount (EUR)","Amount (Foreign Currency)","Type Foreign Currency","Exchange Rate"\n'
    format_2024 = '"Booking Date","Value Date","Partner Name","Partner Iban",Type,"Payment Reference","Account Name","Amount (EUR)","Original Amount","Original Currency","Exchange Rate"\n'

    if first_line == format_2023:
        functions.log(f"Detected format 2023 for {inputFile}.")
        return 1
    elif first_line == format_2024:
        functions.log(f"Detected format 2024 for {inputFile}.")
        return 2

    functions.log(f"Unknown format for {inputFile}.")
    return 0
