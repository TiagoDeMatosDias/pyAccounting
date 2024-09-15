from classes.functions import Functions as functions
from decimal import Decimal
import pandas as pd
import classes.pandas as pandas


def write_Entries(run, config):
    """
    Receives an output file path and writes the entries for the parser to the specified location.

    Parameters:
    - run: The run configuration object.
    - config: A dictionary containing configuration settings.
    """
    try:
        output = functions.get_runParameter(run, "output")
        entries = import_Entries(config["Config_IBKR"])
        pandas.write_file_entries(entries, output, config["CSV_Separator"])
        functions.log("Entries successfully written to output file.")
    except Exception as e:
        functions.log(f"Error in write_Entries: {e}")


def import_Entries(parserLocation):
    """
    Gets the list of files in the input folder and parses them to return all entries.

    Parameters:
    - parserLocation: Path to the JSON configuration file for parsing.

    Returns:
    - A DataFrame containing the sorted entries.
    """
    try:
        parser_config = functions.import_json(parserLocation)
        inputfiles = parser_config["input"]
        inputFolder = functions.get_full_Path(inputfiles)
        inputFiles = functions.get_ListFilesInDir(inputFolder)

        entries = []
        for inputFile in inputFiles:
            entries.extend(get_entriesFromFile(inputFile, parser_config))

        entries = pd.DataFrame(entries)
        entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
        functions.log("Entries successfully imported and sorted.")
        return entries
    except Exception as e:
        functions.log(f"Error in import_Entries: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def get_entriesFromFile(inputFile, parser_config):
    """
    Reads the XML file and returns a list of parsed entries.

    Parameters:
    - inputFile: Path to the XML file.
    - parser_config: Configuration settings for parsing the XML.

    Returns:
    - A list of parsed entries.
    """
    entries = []
    try:
        XML = functions.read_XML(inputFile)
        xmlEntries = functions.get_XML_Tag_All(XML)
        for obj in xmlEntries:
            entryList = parse_transactions(obj, parser_config)
            if entryList:
                entries.extend(entryList)
        functions.log(f"Entries successfully parsed from file: {inputFile}")
    except Exception as e:
        functions.log(f"Error in get_entriesFromFile for {inputFile}: {e}")
    return entries


def parse_transactions(obj, parser_config):
    """
    Parses transactions based on the XML object tag and attributes.

    Parameters:
    - obj: The XML object to parse.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A list of parsed transaction entries.
    """
    entries = []
    try:
        if obj["tag"] == "Trade":
            assetCategory = obj['attrs'].get("assetCategory")
            if assetCategory == "CASH":
                entries.extend(get_CASH(obj['attrs'], parser_config))
            elif assetCategory in ["OPT", "STK"]:
                entries.extend(get_STK(obj['attrs'], parser_config))

        elif obj["tag"] == "Transfer":
            entries.extend(get_Transfer(obj['attrs'], parser_config))

        elif obj["tag"] == "CorporateAction":
            entries.extend(get_CorporateAction(obj['attrs'], parser_config))

        elif obj["tag"] == "CashTransaction" and obj['attrs'].get("levelOfDetail") == "DETAIL":
            txn_type = obj['attrs'].get("type")
            if txn_type == "Broker Interest Paid":
                entries.extend(get_Interest(obj['attrs'], parser_config))
            elif txn_type in ["Other Fees", "Commission Adjustments"]:
                entries.extend(get_Fees(obj['attrs'], parser_config))
            elif txn_type in ["Payment In Lieu Of Dividends", "Dividends"]:
                entries.extend(get_Dividends(obj['attrs'], parser_config))
            elif txn_type == "Withholding Tax":
                entries.extend(get_WithholdingTax(obj['attrs'], parser_config))
            elif txn_type == "Deposits/Withdrawals":
                entries.extend(get_Deposits(obj['attrs'], parser_config))

        functions.log(f"Successfully parsed transactions for tag: {obj['tag']}")
    except Exception as e:
        functions.log(f"Error in parse_transactions for tag {obj['tag']}: {e}")

    return entries


def get_STK(object, parser_config):
    """
    Parses stock transaction data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing stock transaction data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        quantity = object.get('quantity', Decimal('0'))
        symbol = object.get('symbol', 'UnknownSymbol')
        cost = object.get('cost', Decimal('0'))
        currency = object.get('currency', 'UnknownCurrency')
        net_cash = object.get('netCash', Decimal('0'))
        ib_commission = Decimal(object.get('ibCommission', '0')).copy_negate()
        ib_commission_currency = object.get('ibCommissionCurrency', 'UnknownCurrency')
        taxes = Decimal(object.get('taxes', '0')).copy_negate()
        fifo_pnl_realized = Decimal(object.get('fifoPnlRealized', '0')).copy_negate()
        multiplier = Decimal(object.get('multiplier', '1'))
        trade_price = Decimal(object.get('tradePrice', '0'))

        # Entry 1: Transaction for asset purchase
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Equity")
        Quantity.append(quantity)
        Quantity_Type.append(symbol)
        Cost.append(cost)
        Cost_Type.append(currency)

        # Entry 2: Cash transaction for net cash
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(net_cash)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 3: Commission fee
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Expenses{account_separator}{account_subAccount}{account_separator}Fees{account_separator}Trading")
        Quantity.append(ib_commission)
        Quantity_Type.append(ib_commission_currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 4: Taxes
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Expenses{account_separator}Taxes{account_separator}{account_subAccount}{account_separator}Trading")
        Quantity.append(taxes)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 5: Profit and Loss
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(f"{symbol} - {description}")
        Account.append(f"Income{account_separator}{account_subAccount}{account_separator}PnL")
        Quantity.append(fifo_pnl_realized + ib_commission)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Price Update
        price_per_share = trade_price * multiplier
        Date.append(report_date)
        Type.append("PriceUpdate")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append("")
        Quantity.append(None)
        Quantity_Type.append(symbol)
        Cost.append(price_per_share)
        Cost_Type.append(currency)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed STK data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_STK parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }


def get_CASH(object, parser_config):
    """
    Parses cash transaction data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing cash transaction data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        proceeds = Decimal(object.get('proceeds', '0'))
        quantity = Decimal(object.get('quantity', '0'))
        ib_commission = Decimal(object.get('ibCommission', '0'))
        taxes = Decimal(object.get('taxes', '0'))
        multiplier = Decimal(object.get('multiplier', '1'))
        trade_price = Decimal(object.get('tradePrice', '0'))
        selling_currency = object.get('symbol', '').split(".")[0]
        ib_commission_currency = object.get('ibCommissionCurrency', 'UnknownCurrency')
        currency = object.get('currency', 'UnknownCurrency')

        # Entry 1: Cash proceeds
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(proceeds)
        Quantity_Type.append(currency)
        Cost.append(-quantity)  # Negative quantity
        Cost_Type.append(selling_currency)

        # Entry 2: Quantity of stock
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(quantity)
        Quantity_Type.append(selling_currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 3: IB Commission
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(ib_commission)
        Quantity_Type.append(ib_commission_currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 4: IB Commission Expense
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Expenses{account_separator}{account_subAccount}{account_separator}Fees{account_separator}Cash")
        Quantity.append(-ib_commission)  # Negative commission
        Quantity_Type.append(ib_commission_currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 5: Taxes
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Expenses{account_separator}Taxes{account_separator}{account_subAccount}{account_separator}Cash")
        Quantity.append(taxes)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Price Update 1: Price in selling currency
        price_per_share = trade_price * multiplier
        Date.append(report_date)
        Type.append("PriceUpdate")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append("")
        Quantity.append(None)
        Quantity_Type.append(selling_currency)
        Cost.append(price_per_share)
        Cost_Type.append(currency)

        # Price Update 2: Price in transaction currency
        Date.append(report_date)
        Type.append("PriceUpdate")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append("")
        Quantity.append(None)
        Quantity_Type.append(currency)
        Cost.append(1 / price_per_share)
        Cost_Type.append(selling_currency)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed CASH data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_CASH parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }


def get_Transfer(object, parser_config):
    """
    Parses transfer transaction data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing transfer transaction data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        quantity = Decimal(object.get('quantity', '0'))
        symbol = object.get('symbol', 'UnknownSymbol')
        position_amount = Decimal(object.get('positionAmount', '0'))
        currency = object.get('currency', 'UnknownCurrency')
        transfer_type = object.get('type', 'UnknownType')

        # Entry 1: Transfer to equity
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Equity")
        Quantity.append(quantity)
        Quantity_Type.append(symbol)
        Cost.append(position_amount)
        Cost_Type.append(currency)

        # Entry 2: Transfer from equity
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}{account_subAccount}{account_separator}{transfer_type}")
        Quantity.append(-quantity)  # Negative quantity for the outflow
        Quantity_Type.append(symbol)
        Cost.append(position_amount)
        Cost_Type.append(currency)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed Transfer data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_Transfer parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }


def get_CorporateAction(object, parser_config):
    """
    Parses corporate action data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing corporate action data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        quantity = Decimal(object.get('quantity', '0'))
        symbol = object.get('symbol', 'UnknownSymbol')

        # Entry 1: Corporate Action to Equity
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Equity")
        Quantity.append(quantity)
        Quantity_Type.append(symbol)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Corporate Action as Income
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Income{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}CorporateActions")
        Quantity.append(-quantity)  # Negative quantity for outflow
        Quantity_Type.append(symbol)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed CorporateAction data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_CorporateAction parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }


def get_Deposits(object, parser_config):
    """
    Parses deposit transaction data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing deposit transaction data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        amount = Decimal(object.get('amount', '0'))
        currency = object.get('currency', 'UnknownCurrency')

        # Entry 1: Deposit to Cash
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(
            f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(amount)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Transfer from Cash to IBKR
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}Transfers{account_separator}IBKR")
        Quantity.append(-amount)  # Negative amount for outflow
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed Deposits data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_Deposits parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }

def get_WithholdingTax(object, parser_config):
    """
    Parses withholding tax data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing withholding tax data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        amount = Decimal(object.get('amount', '0'))
        currency = object.get('currency', 'UnknownCurrency')
        symbol = object.get('symbol', 'UnknownSymbol')

        # Entry 1: Withholding tax to Cash
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(amount)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Withholding tax as Expense
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(f"{symbol} - {description}")
        Account.append(f"Expenses{account_separator}Taxes{account_separator}{account_subAccount}{account_separator}Dividend_Withholding")
        Quantity.append(-amount)  # Negative amount for expense
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed WithholdingTax data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_WithholdingTax parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }
def get_Dividends(object, parser_config):
    """
    Parses dividend data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing dividend data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        amount = Decimal(object.get('amount', '0'))
        currency = object.get('currency', 'UnknownCurrency')
        symbol = object.get('symbol', 'UnknownSymbol')

        # Entry 1: Dividend to Cash
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(amount)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Dividend as Income
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Income{account_separator}{account_subAccount}{account_separator}Dividends{account_separator}{symbol}")
        Quantity.append(-amount)  # Negative amount for income
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed Dividends data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_Dividends parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }

def get_Fees(object, parser_config):
    """
    Parses fee data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing fee data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        amount = Decimal(object.get('amount', '0'))
        currency = object.get('currency', 'UnknownCurrency')

        # Entry 1: Fees as Cash Outflow
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(amount)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Fees as Expense
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Expenses{account_separator}{account_subAccount}{account_separator}Fees{account_separator}Other")
        Quantity.append(-amount)  # Negative amount for expense
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed Fees data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_Fees parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }


def get_Interest(object, parser_config):
    """
    Parses interest data from an XML object and returns it in a structured format.

    Parameters:
    - object: The XML object containing interest data.
    - parser_config: Configuration settings for parsing the XML object.

    Returns:
    - A dictionary containing parsed entries with columns for Date, Type, ID, Name, Account, Quantity, Quantity_Type, Cost, and Cost_Type.
    """
    # Initialize lists to store column data
    Date = []
    Type = []
    ID = []
    Name = []
    Account = []
    Quantity = []
    Quantity_Type = []
    Cost = []
    Cost_Type = []

    try:
        # Configuration parameters
        account_separator = parser_config.get("AccountSeparator", "/")
        account_subAccount = parser_config.get("SubAccounts", "")

        # Extract values from XML object
        transaction_id = object.get('transactionID', 'UnknownID')
        report_date = object.get('reportDate', 'UnknownDate')
        description = object.get('description', 'No Description')
        account_id = object.get('accountId', 'UnknownAccount')
        amount = Decimal(object.get('amount', '0'))
        currency = object.get('currency', 'UnknownCurrency')

        # Entry 1: Interest as Cash Inflow
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Assets{account_separator}{account_subAccount}{account_separator}{account_id}{account_separator}Cash")
        Quantity.append(amount)
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Entry 2: Interest as Expense
        Date.append(report_date)
        Type.append("Transaction")
        ID.append(f"IBKR_{transaction_id}")
        Name.append(description)
        Account.append(f"Expenses{account_separator}{account_subAccount}{account_separator}Interest")
        Quantity.append(-amount)  # Negative amount for expense
        Quantity_Type.append(currency)
        Cost.append(None)
        Cost_Type.append(None)

        # Join the data into a dictionary
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

        functions.log(f"Successfully parsed Interest data for transaction ID: {transaction_id}")
        return entries

    except Exception as e:
        functions.log(f"Error in get_Interest parsing: {e}")
        # Return an empty dictionary or handle it as needed
        return {
            "Date": [],
            "Type": [],
            "ID": [],
            "Name": [],
            "Account": [],
            "Quantity": [],
            "Quantity_Type": [],
            "Cost": [],
            "Cost_Type": [],
        }
