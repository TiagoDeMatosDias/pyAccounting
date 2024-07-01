import pandas as pd
from classes.functions import Functions as f

def read_file(filepath, separator):
    entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator, parse_dates=["Date"], date_format="%Y-%m-%d")
    return entries

def write_file_entries(entries, output, separator):
    outputfile = f.get_full_Path(output)
    entries.to_csv(outputfile, sep=separator, index=False, mode="w", header=True,columns=["Date","Type","ID","Name","Account","Quantity","Quantity_Type","Cost","Cost_Type"])
    pass

def write_file_balance(entries, output, separator):
    outputfile = f.get_full_Path(output)
    entries.to_csv(outputfile, sep=separator, index=False, mode="w", header=True)
    pass


def get_runningTotal(entries, date, Account, Quantity_Type, sumColumn):

    filtered = entries.loc[(entries['Date'] <= date) & (entries['Account'] == Account) & (entries['Quantity_Type'] == Quantity_Type) ]
    if len(filtered) == 0:
        return 0
    else:
        return filtered[sumColumn].sum()

def get_crossJoinedFrames(frame_1, frame_2):
    frame_1["key"] = 0
    frame_2["key"] = 0
    return frame_1.merge(frame_2, on="key", how="outer")

def get_dateFrame(Start, End, Frequency):
    Dates = pd.date_range(Start, End, freq=Frequency)
    return pd.DataFrame({"Date": Dates})

def get_uniqueFrame(entries, column):
    unique = entries[column].unique()
    return pd.DataFrame({column: unique})
