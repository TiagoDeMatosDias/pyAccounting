from classes.functions import Functions as functions
from decimal import Decimal
import pandas as pd
import classes.pandas as pandas


def write_Entries(run, config):
    output = functions.get_runParameter(run, "output")
    entries = import_Entries(config["Config_wise"])
    pandas.write_file_entries(entries, output, config["CSV_Separator"])
    pass

def import_Entries(parserLocation):
    parser_config = functions.import_json(parserLocation)
    inputfiles = parser_config["input"]
    inputFolder = functions.get_full_Path(inputfiles)
    inputFiles = functions.get_ListFilesInDir(inputFolder)
    try:
        rules = pd.read_csv(filepath_or_buffer=parser_config['RulesTable'], sep=parser_config['RulesTable_Separator'])
    except:
        functions.log("No rules available at " + parser_config['RulesTable'])
        return pd.DataFrame()

    entries = []
    for inputFile in inputFiles:
        entries = functions.combine_lists(get_entriesFromFile(inputFile, parser_config,rules), entries)

    entries = pd.DataFrame(entries)
    try:
        entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
    except:
        functions.log("Unable to sort values for Wise entries")
    return entries

    pass


def get_entriesFromFile(inputFile, parser_config, rules):
    entries = []
    data = pd.read_csv(filepath_or_buffer=inputFile, sep=parser_config["separator"], parse_dates=[parser_config["DateColumn"]], date_format=parser_config["DateFormat"])

    for index, row in data.iterrows():
        entries = functions.combine_lists(convert_transaction(row, parser_config, rules), entries)

    return entries

def convert_transaction(row, parser_config, rules):
    ## Columns
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
    name = str(str(row["Description"])).replace(" ", "")

    account = get_Account(name, parser_config, rules)
    id = "Wise_" + row["TransferWise ID"]

    # entry 1
    Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
    Type.append("Transaction")
    ID.append(id)
    Name.append(name)
    Account.append(account_subAccount)
    Quantity.append(row["Amount"])
    Quantity_Type.append(row['Currency'])
    Cost.append(None)
    Cost_Type.append(None)
    # entry 2
    if pd.isna(row['Exchange From']) :
        Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
        Type.append("Transaction")
        ID.append(id)
        Name.append(name)
        Account.append(account)
        Quantity.append(Decimal(row["Amount"]).copy_negate())
        Quantity_Type.append(row['Currency'])
        Cost.append(None)
        Cost_Type.append(None)

    else:
        Date.append(pd.to_datetime(row["Date"], format=parser_config['DateFormat']))
        Type.append("Transaction")
        ID.append(id)
        Name.append(name)
        Account.append(account)
        Quantity.append(row['Exchange To Amount'])
        Quantity_Type.append(row['Exchange To'])
        Cost.append(Decimal(row["Amount"]).copy_negate())
        Cost_Type.append(row['Currency'])

    ##Join the data
    entries = {
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

    return entries



def get_Account(name, parser_config, rules):
    out = parser_config['UndefinedAccount']
    for index, row, in rules.iterrows():
        if row["Source"] in name:
            if row["Exact"] == True:
                if row["Source"] == name:
                    out = row["Account"]
                    return out
            else:
                out = row["Account"]
                return out
    return out


