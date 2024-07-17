import yfinance as yf
from classes.functions import Functions as f
import classes.pandas as pandas
import pandas as pd

def get_tickerData(ticker, yticker, interval, date_min, date_max):
    if yticker == None:
        yticker = ticker


    t = yf.Ticker(yticker)

    data = t.history(period="max", interval=interval)

    data =  pd.DataFrame(data).reset_index()
    data["Date"] = data["Date"].dt.date
    data["Date"] = pd.to_datetime(data["Date"])

    data = f.filter_data(data, "Min", "Date", date_min)
    data = f.filter_data(data, "Max", "Date", date_max)



    entries = {
        "Date": data["Date"],
        "Type": "PriceUpdate",
        "ID": "",
        "Name": "",
        "Account": "",
        "Quantity": "",
        "Quantity_Type": ticker,
        "Cost": data["Close"],
        "Cost_Type": t.info["currency"],
    }

    return entries

def get_PriceUpdates(run):

    interval = f.get_runParameter(run ,"interval")
    date_min = f.get_runParameter(run ,"date_min")
    date_max = f.get_runParameter(run ,"date_max")
    entries = pd.DataFrame()

    for update in run["Tickers"]:
        Ticker = f.get_runParameter(update, "Ticker")
        yTicker = f.get_runParameter(update, "yTicker")

        updatedata = pd.DataFrame(get_tickerData(Ticker, yTicker, interval, date_min, date_max))

        if len(updatedata) > 0:
            entries = pd.concat([entries, updatedata], ignore_index=True)
    entries.reset_index()
    entries = entries[entries.Date != ""]

    return entries


def write_Entries(run, config):
    try:
        entries = get_PriceUpdates(run)
        output = f.get_runParameter(run ,"output")
        CSV_Separator = f.get_runParameter(config ,"CSV_Separator")

        import classes.pandas as pandas
        pandas.write_file_entries(entries, output, CSV_Separator)

    except AttributeError:
        f.log(AttributeError.obj)
        f.log(AttributeError.name)
        f.log(AttributeError.args)

    pass