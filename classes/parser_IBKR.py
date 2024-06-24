from classes.functions import Functions as functions
from classes.Entry import Entry as entry
from decimal import Decimal

## This function receives an output file path (relative path) and writes the entries for that parser in the location
def register_Entries(output, config):
    entries = import_Entries()
    outputfile = functions.get_full_Path(output)

    functions.write_CSV(outputfile, entries, config["CSV_Separator"], True, True)

## This function gets the list of files in the input folder and proceeds to parse them one by one, returning the entries it found in all of them
def import_Entries():
    parser_config = functions.import_json('Files/config/parser_IBKR.json')
    inputfiles= parser_config["input"]
    inputFolder = functions.get_full_Path(inputfiles)
    inputFiles = functions.get_ListFilesInDir(inputFolder)

    entries = []
    for inputFile in inputFiles:
        entries = entries + get_entriesFromFile(inputFile, parser_config)

    return entries

## This function reads the IBKR XML file and returns a list of every XML object
def get_entriesFromFile(inputFile, parser_config):
    entries = []
    XML = functions.read_XML(inputFile)
    xmlEntries = functions.get_XML_Tag_All(XML)
    for object in xmlEntries:
        entries = entries + parse_transactions(object, parser_config)
    return entries


## This function selects which type of XML object it is, and picks the appropriate function to parse it
def parse_transactions(object, parser_config):
    entries = []
    if object["tag"] == "Trade":
        if object['attrs']["assetCategory"] == "CASH":
            entries = entries + get_CASH(object['attrs'], parser_config)
            pass
        elif (object['attrs']["assetCategory"] == "OPT") or (object['attrs']["assetCategory"] == "STK"):
            entries = entries + get_STK(object['attrs'], parser_config)
            pass

    if object["tag"] == "Transfer":
            entries = entries + get_Transfer(object['attrs'], parser_config)

    if object["tag"] == "CorporateAction":
            entries = entries + get_CorporateAction(object['attrs'], parser_config)

    if object["tag"] == "CashTransaction" and object['attrs']["levelOfDetail"] == "DETAIL":
        if object['attrs']["type"] == "Broker Interest Paid":
            entries = entries + get_Interest(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Other Fees") or (object['attrs']["type"] == "Commission Adjustments"):
            entries = entries + get_Fees(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Payment In Lieu Of Dividends") or (object['attrs']["type"] == "Dividends"):
            entries = entries + get_Dividends(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Withholding Tax") :
            entries = entries + get_WithholdingTax(object['attrs'], parser_config)
            pass
        elif (object['attrs']["type"] == "Deposits/Withdrawals") :
            entries = entries + get_Deposits(object['attrs'], parser_config)
            pass
    return entries


## Everything below this handles the parsing of individual XML objects, which represent purchases/sales of securities, dividend payments, etc....
def get_STK(object ,parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Equity"   + account_separator + object['symbol']
    Quantity = object['quantity']
    Quantity_Type = object['symbol']
    Cost = object['cost']
    Cost_Type = object['currency']
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 2
    Account = "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash"
    Quantity = object['netCash']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 3
    Account = "Expenses" + account_separator + account_subAccount  + account_separator + "Fees"   + account_separator + object['symbol']
    Quantity = Decimal( object['ibCommission']).copy_negate()
    Quantity_Type = object['ibCommissionCurrency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 4
    Account = "Expenses" + account_separator + "Taxes"  + account_separator + account_subAccount   + account_separator + object['symbol']
    Quantity = Decimal( object['taxes']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))

    # entry 5
    Account = "Income" + account_separator + account_subAccount + account_separator + "PnL"  + account_separator + object['symbol']
    Quantity = Decimal( object['fifoPnlRealized']).copy_negate() + Decimal(object['ibCommission'])
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))

    # Price Update
    multiplier = Decimal( object['multiplier'] )
    pricepershare = Decimal( object['tradePrice'])*multiplier
    entries.append(entry(Date, "PriceUpdate", ID, Name, "", None, object['symbol'] , pricepershare, object['currency'] ))


    return entries

def get_CASH(object ,parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount =  parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']
    selling_Currency =  object['symbol'].split(".")[0]



    # entry 1
    Account = "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash"
    Quantity = object['proceeds']
    Quantity_Type = object['currency']
    Cost = Decimal(object['quantity']).copy_negate()
    Cost_Type = selling_Currency
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 2
    Account = "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash"
    Quantity = object['quantity']
    Quantity_Type = selling_Currency
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 3
    Account = "Assets" + account_separator + account_subAccount  + account_separator + object['accountId']  + account_separator + "Cash"
    Quantity = object['ibCommission']
    Quantity_Type = object['ibCommissionCurrency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))


    # entry 4
    Account = "Expenses" + account_separator + account_subAccount  + account_separator + "Fees"   + account_separator + object['symbol']
    Quantity = Decimal( object['ibCommission']).copy_negate()
    Quantity_Type = object['ibCommissionCurrency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))

    # entry 5
    Account = "Expenses" + account_separator + "Taxes"  + account_separator + account_subAccount   + account_separator + object['symbol']
    Quantity = Decimal( object['taxes'])
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type ))

    # Price Update 1
    multiplier = Decimal( object['multiplier'] )
    pricepershare = Decimal( object['tradePrice'])*multiplier
    entries.append(entry(Date, "PriceUpdate", ID, Name, "", None, selling_Currency , pricepershare, object['currency'] ))
    entries.append(entry(Date, "PriceUpdate", ID, Name, "", None, object['currency'] , 1/pricepershare, selling_Currency ))

    return entries

def get_Transfer(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Equity" + account_separator + object['symbol']
    Quantity = object['quantity']
    Quantity_Type = object['symbol']
    Cost =object['positionAmount']
    Cost_Type = object['currency']
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Assets" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator + object['type']
    Quantity = Decimal(object['quantity']).copy_negate()
    Quantity_Type = object['symbol']
    Cost = object['positionAmount']
    Cost_Type =  object['currency']
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))



    return entries

def get_CorporateAction(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Equity" + account_separator + object['symbol']
    Quantity = object['quantity']
    Quantity_Type = object['symbol']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Income" + account_separator + account_subAccount + account_separator + object['accountId'] + account_separator + "CorporateActions"+ account_separator + object['symbol']
    Quantity = Decimal(object['quantity']).copy_negate()
    Quantity_Type = object['symbol']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))



    return entries
def get_Deposits(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash"
    Quantity = object['amount']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Assets" + account_separator + "Transfers" + account_separator + "IBKR"
    Quantity = Decimal(object['amount']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    return entries

def get_WithholdingTax(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash"
    Quantity = object['amount']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Expenses" + account_separator + "Taxes" + account_separator + account_subAccount + account_separator + "Dividend_Withholding"+ account_separator + object['symbol']
    Quantity = Decimal(object['amount']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    return entries

def get_Dividends(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash"
    Quantity = object['amount']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Income" + account_separator + account_subAccount + account_separator + "Dividends"+ account_separator + object['symbol']
    Quantity = Decimal(object['amount']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    return entries

def get_Fees(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash"
    Quantity = object['amount']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Expenses" + account_separator + account_subAccount + account_separator + "Fees"+ account_separator + object['symbol']
    Quantity = Decimal(object['amount']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    return entries

def get_Interest(object, parser_config):
    entries = []
    account_separator = parser_config["AccountSeparator"]
    account_subAccount = parser_config["SubAccounts"]

    # Global Parameters
    Date = object['reportDate']
    Type = "Transaction"
    ID = object['transactionID']
    Name = object['description']

    # entry 1
    Account = "Assets" + account_separator + account_subAccount + account_separator + object[
        'accountId'] + account_separator + "Cash"
    Quantity = object['amount']
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type = None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    # entry 2
    Account = "Expenses" + account_separator + account_subAccount + account_separator + "Interest"
    Quantity = Decimal(object['amount']).copy_negate()
    Quantity_Type = object['currency']
    Cost = None
    Cost_Type =  None
    entries.append(entry(Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, Cost_Type))

    return entries