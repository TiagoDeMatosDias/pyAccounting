import pandas as pd
from classes.functions import Functions as f

def read_file(filepath, separator):
    entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator, parse_dates=["Date"], date_format="%Y-%m-%d")
    return entries

def write_file_entries(entries, output, separator):
    outputfile = f.get_full_Path(output)
    entries.to_csv(outputfile, sep=separator, index=False, mode="w", header=True,columns=["Date","Type","ID","Name","Account","Quantity","Quantity_Type","Cost","Cost_Type"])
    pass

