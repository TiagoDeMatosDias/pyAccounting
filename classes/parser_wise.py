from classes.functions import Functions as functions
from decimal import Decimal
import pandas as pd
import classes.pandas as pandas

def write_Entries(run, config):
    """
    Main function to write entries to a file based on the provided configuration.

    Parameters:
    - run: Identifier for the run configuration.
    - config: Configuration settings for the process.
    """
    output = functions.get_runParameter(run, "output")
    entries = import_Entries(config["Config_wise"])
    pandas.write_file(entries, output, config["CSV_Separator"])

def import_Entries(parserLocation):
    """
    Imports entries from input files based on the configuration.

    Parameters:
    - parserLocation: Path to the JSON configuration file.

    Returns:
    - A DataFrame containing all parsed entries.
    """
    parser_config = functions.import_json(parserLocation)
    inputfiles = parser_config["input"]
    inputFolder = functions.get_full_Path(inputfiles)
    inputFiles = functions.get_ListFilesInDir(inputFolder)

    # Load rules table
    try:
        rules = pd.read_csv(filepath_or_buffer=parser_config['RulesTable'], sep=parser_config['RulesTable_Separator'])
    except Exception as e:
        functions.log(f"Error loading rules table: {e}")
        return pd.DataFrame()

    entries = []
    for inputFile in inputFiles:
        try:
            file_entries = get_entriesFromFile(inputFile, parser_config, rules)
            entries.extend(file_entries)
        except Exception as e:
            functions.log(f"Error processing file {inputFile}: {e}")

    entries_df = pd.DataFrame(entries)
    try:
        entries_df = entries_df.sort_values(by="Date", ascending=True).reset_index(drop=True)
    except Exception as e:
        functions.log(f"Unable to sort values for entries: {e}")

    return entries_df

def get_entriesFromFile(inputFile, parser_config, rules):
    """
    Extracts entries from a single file.

    Parameters:
    - inputFile: Path to the input file.
    - parser_config: Configuration settings.
    - rules: Rules DataFrame for account mapping.

    Returns:
    - A list of entries parsed from the file.
    """
    entries = []
    try:
        data = pd.read_csv(
            filepath_or_buffer=inputFile,
            sep=parser_config["separator"],
            parse_dates=[parser_config["DateColumn"]],
            date_format=parser_config["DateFormat"]
        )
    except Exception as e:
        functions.log(f"Error reading file {inputFile}: {e}")
        return entries

    for index, row in data.iterrows():
        try:
            row_entries = convert_transaction(row, parser_config, rules)
            entries.extend(row_entries)
        except Exception as e:
            functions.log(f"Error processing row {index} in file {inputFile}: {e}")

    return entries

def convert_transaction(row, parser_config, rules):
    """
    Converts a single row of data into multiple transaction entries.

    Parameters:
    - row: The row of data to convert.
    - parser_config: Configuration settings.
    - rules: Rules DataFrame for account mapping.

    Returns:
    - A list of transaction entries derived from the row.
    """
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    account_subAccount = parser_config["SubAccounts"]
    name = str(row["Description"]).replace(" ", "").replace(",", "").upper()

    account = get_Account(name, parser_config, rules)
    transaction_id = "Wise_" + str(row["TransferWise ID"])

    # Entry 1: Direct transaction
    Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
    Type.append("Transaction")
    ID.append(transaction_id)
    Name.append(name)
    Account.append(account_subAccount)
    Quantity.append(row["Amount"])
    Quantity_Type.append(row['Currency'])
    Cost.append(None)
    Cost_Type.append(None)

    # Entry 2: Exchange-related transaction
    if pd.isna(row['Exchange From']):
        Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
        Type.append("Transaction")
        ID.append(transaction_id)
        Name.append(name)
        Account.append(account)
        Quantity.append(Decimal(row["Amount"]).copy_negate())
        Quantity_Type.append(row['Currency'])
        Cost.append(None)
        Cost_Type.append(None)
    else:
        Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
        Type.append("Transaction")
        ID.append(transaction_id)
        Name.append(name)
        Account.append(account)
        Quantity.append(row['Exchange To Amount'])
        Quantity_Type.append(row['Exchange To'])
        Cost.append(Decimal(row["Amount"]).copy_negate())
        Cost_Type.append(row['Currency'])

    # Combine entries into a list of dictionaries
    entries = [{
        "Date": Date[i],
        "Type": Type[i],
        "ID": ID[i],
        "Name": Name[i],
        "Account": Account[i],
        "Quantity": Quantity[i],
        "Quantity_Type": Quantity_Type[i],
        "Cost": Cost[i],
        "Cost_Type": Cost_Type[i]
    } for i in range(len(Date))]

    return entries

def get_Account(name, parser_config, rules):
    """
    Determines the appropriate account based on the transaction name and rules.

    Parameters:
    - name: The transaction name.
    - parser_config: Configuration settings.
    - rules: Rules DataFrame for account mapping.

    Returns:
    - The determined account as a string.
    """
    default_account = parser_config.get('UndefinedAccount', 'Undefined')
    for _, row in rules.iterrows():
        if row["Source"].upper() in name:
            if row.get("Exact", False):
                if row["Source"].upper() == name:
                    return row["Account"]
            else:
                return row["Account"]
    return default_account
