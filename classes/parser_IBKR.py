from classes.functions import Functions as functions
from decimal import Decimal
import pandas as pd
import classes.pandas as pandas


## This function receives an output file path (relative path) and writes the entries for that parser in the location
def write_Entries(run, config):
    output = functions.get_runParameter(run, "output")
    entries = import_Entries(config["Config_IBKR"])
    pandas.write_file(entries, output, config["CSV_Separator"])

    pass


## This function gets the list of files in the input folder and proceeds to parse them one by one, returning the entries it found in all of them
def import_Entries(parserLocation):
    parser_config = functions.import_json(parserLocation)
    inputfiles= parser_config["input"]
    inputFolder = functions.get_full_Path(inputfiles)
    inputFiles = functions.get_ListFilesInDir(inputFolder)

    entries = []
    for inputFile in inputFiles:
        entries = functions.combine_lists(entries, get_entriesFromFile(inputFile, parser_config))

    entries = pd.DataFrame(entries)
    try:
        entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
    except:
        functions.log("Unable to sort values for Wise entries")
    return entries
    return entries

## This function reads the IBKR XML file and returns a list of every XML object
def get_entriesFromFile(inputFile, parser_config):
    entries = []
    XML = functions.read_XML(inputFile)
    xmlEntries = functions.get_XML_Tag_All(XML)
    for object in xmlEntries:
        entryList = parse_transactions(object, parser_config)
        if len(entryList) != 0:
            entries = functions.combine_lists(entries,entryList)

    return entries


## This function selects which type of XML object it is, and picks the appropriate function to parse it
def parse_transactions(object, parser_config):
    entries = []

    if object["tag"] == "Trade":
        if object['attrs']["assetCategory"] == "CASH":
            entries = functions.combine_lists(entries, get_CASH(object['attrs'], parser_config))
            pass
        elif (object['attrs']["assetCategory"] == "OPT") or (object['attrs']["assetCategory"] == "STK"):
            entries = functions.combine_lists(entries, get_STK(object['attrs'], parser_config))

            pass

    if object["tag"] == "Transfer":
            entries = functions.combine_lists(entries, get_Transfer(object['attrs'], parser_config))


    if object["tag"] == "CorporateAction":
            entries = functions.combine_lists(entries, get_CorporateAction(object['attrs'], parser_config))
            # entries = entries +   get_CorporateAction(object['attrs'], parser_config)

    if object["tag"] == "CashTransaction" and object['attrs']["levelOfDetail"] == "DETAIL":
        if object['attrs']["type"] == "Broker Interest Paid":
            entries = functions.combine_lists(entries, get_Interest(object['attrs'], parser_config))

            #entries = entries +   get_Interest(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Other Fees") or (object['attrs']["type"] == "Commission Adjustments"):
            entries = functions.combine_lists(entries, get_Fees(object['attrs'], parser_config))

            #entries = entries +   get_Fees(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Payment In Lieu Of Dividends") or (object['attrs']["type"] == "Dividends"):
            entries = functions.combine_lists(entries, get_Dividends(object['attrs'], parser_config))

            #entries = entries +   get_Dividends(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Withholding Tax") :
            entries = functions.combine_lists(entries, get_WithholdingTax(object['attrs'], parser_config))

            #entries = entries +    get_WithholdingTax(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Deposits/Withdrawals") :
            entries = functions.combine_lists(entries, get_Deposits(object['attrs'], parser_config))

            #entries = entries +    get_Deposits(object['attrs'], parser_config)
            pass


    return entries


## Everything below this handles the parsing of individual XML objects, which represent purchases/sales of securities, dividend payments, etc....
def get_STK(object ,parser_config):

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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]

    # Global Parameters

    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Equity" )
    Quantity.append(object['quantity'])
    Quantity_Type.append(object['symbol'])
    Cost.append(object['cost'])
    Cost_Type.append(object['currency'])


    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append("Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash")
    Quantity.append( object['netCash'])
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append( None)


    # entry 3
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Expenses" + account_separator + account_subAccount  + account_separator + "Fees"   + account_separator + "Trading")
    Quantity.append( Decimal( object['ibCommission']).copy_negate())
    Quantity_Type.append( object['ibCommissionCurrency'])
    Cost.append( None)
    Cost_Type.append( None)


    # entry 4
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Expenses" + account_separator + "Taxes"  + account_separator + account_subAccount    + account_separator + "Trading")
    Quantity.append(Decimal( object['taxes']).copy_negate())
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append( None)

    # entry 5
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['symbol'] + " - " + object['description'])
    Account.append( "Income" + account_separator + account_subAccount + account_separator + "PnL"  )
    Quantity.append( Decimal( object['fifoPnlRealized']).copy_negate() + Decimal(object['ibCommission']))
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(  None)

    # Price Update
    multiplier = Decimal( object['multiplier'] )
    pricepershare = Decimal( object['tradePrice'])*multiplier

    Date.append( object['reportDate'])
    Type.append( "PriceUpdate")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append("")
    Quantity.append(None)
    Quantity_Type.append(object['symbol'])
    Cost.append(pricepershare)
    Cost_Type.append(object['currency'])

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_CASH(object ,parser_config):

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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]


    # Global Parameters
    selling_Currency =  object['symbol'].split(".")[0]



    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append("Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash")
    Quantity.append(object['proceeds'])
    Quantity_Type.append(object['currency'])
    Cost.append(Decimal(object['quantity']).copy_negate())
    Cost_Type.append(selling_Currency)



    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash")
    Quantity.append( object['quantity'])
    Quantity_Type.append( selling_Currency)
    Cost.append( None)
    Cost_Type.append( None)


    # entry 3
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash")
    Quantity.append( object['ibCommission'])
    Quantity_Type.append( object['ibCommissionCurrency'])
    Cost.append( None)
    Cost_Type.append( None)


    # entry 4
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Expenses" + account_separator + account_subAccount  + account_separator + "Fees"   + account_separator + "Cash")
    Quantity.append( Decimal( object['ibCommission']).copy_negate())
    Quantity_Type.append( object['ibCommissionCurrency'])
    Cost.append(  None)
    Cost_Type.append(  None)

    # entry 5
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Expenses" + account_separator + "Taxes"  + account_separator + account_subAccount    + account_separator + "Cash")
    Quantity.append( Decimal( object['taxes']))
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append( None)

    # Price Update 1
    multiplier = Decimal( object['multiplier'] )
    pricepershare = Decimal( object['tradePrice'])*multiplier

    Date.append( object['reportDate'])
    Type.append( "PriceUpdate")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append("")
    Quantity.append(None)
    Quantity_Type.append(selling_Currency)
    Cost.append(pricepershare)
    Cost_Type.append(object['currency'])

    # Price Update 2
    multiplier = Decimal( object['multiplier'] )
    pricepershare = Decimal( object['tradePrice'])*multiplier

    Date.append( object['reportDate'])
    Type.append( "PriceUpdate")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append("")
    Quantity.append(None)
    Quantity_Type.append(object['currency'])
    Cost.append(1/pricepershare)
    Cost_Type.append(selling_Currency)


    ##Join the data
    entries = {
        "Date" : Date,
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

def get_Transfer(object, parser_config):

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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]



    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator  + "Equity" )
    Quantity.append( object['quantity'])
    Quantity_Type.append( object['symbol'])
    Cost.append(object['positionAmount'])
    Cost_Type.append( object['currency'])

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Assets" + account_separator + account_subAccount + account_separator + object['type'])
    Quantity.append(  Decimal(object['quantity']).copy_negate())
    Quantity_Type.append(  object['symbol'])
    Cost.append(  object['positionAmount'])
    Cost_Type.append(   object['currency'])

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_CorporateAction(object, parser_config):

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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]



    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator + "Equity" )
    Quantity.append( object['quantity'])
    Quantity_Type.append( object['symbol'])
    Cost.append( None)
    Cost_Type.append( None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Income" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator + "CorporateActions")
    Quantity.append(  Decimal(object['quantity']).copy_negate())
    Quantity_Type.append(  object['symbol'])
    Cost.append(  None)
    Cost_Type.append(   None)

    ##Join the data
    entries = {
        "Date" : Date,
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
def get_Deposits(object, parser_config):
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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]


    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount + account_separator + object[ 'accountId'] + account_separator + "Cash")
    Quantity.append( object['amount'])
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append( None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Assets" + account_separator + "Transfers" + account_separator + "IBKR")
    Quantity.append(  Decimal(object['amount']).copy_negate())
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(   None)

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_WithholdingTax(object, parser_config):
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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator + "Cash")
    Quantity.append( object['amount'])
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append( None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['symbol'] + " - " + object['description'])
    Account.append( "Expenses" + account_separator + "Taxes" + account_separator + account_subAccount + account_separator + "Dividend_Withholding")
    Quantity.append( Decimal(object['amount']).copy_negate())
    Quantity_Type.append( object['currency'])
    Cost.append( None)
    Cost_Type.append(  None)

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_Dividends(object, parser_config):
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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Assets" + account_separator + account_subAccount + account_separator + object[ 'accountId'] + account_separator + "Cash")
    Quantity.append(  object['amount'])
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(  None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Income" + account_separator + account_subAccount + account_separator + "Dividends"+ account_separator + object['symbol'])
    Quantity.append(  Decimal(object['amount']).copy_negate())
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(   None)

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_Fees(object, parser_config):
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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]


    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append( "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash")
    Quantity.append(  object['amount'])
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append( None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Expenses" + account_separator + account_subAccount + account_separator + "Fees" + account_separator + "Other")
    Quantity.append(  Decimal(object['amount']).copy_negate())
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(   None)

    ##Join the data
    entries = {
        "Date" : Date,
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

def get_Interest(object, parser_config):
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

    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]


    # entry 1
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash")
    Quantity.append(  object['amount'])
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(  None)

    # entry 2
    Date.append( object['reportDate'])
    Type.append( "Transaction")
    ID.append( "IBKR_" + object['transactionID'])
    Name.append( object['description'])
    Account.append(  "Expenses" + account_separator + account_subAccount + account_separator + "Interest")
    Quantity.append(  Decimal(object['amount']).copy_negate())
    Quantity_Type.append(  object['currency'])
    Cost.append(  None)
    Cost_Type.append(   None)

    ##Join the data
    entries = {
        "Date" : Date,
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