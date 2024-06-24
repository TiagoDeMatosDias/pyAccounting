import pandas as pd

def get_panda(filepath, separator):
    entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator,parse_dates=["Date"])
    return entries

def get_Prices(entries):
    return entries[entries["Type"] == "PriceUpdate"]


def write_panda(entries, output, separator):
    outputfile = functions.get_full_Path(output)

    entries.to_csv(outputfile, sep=separator)
    pass

