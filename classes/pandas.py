import pandas as pd
from classes.functions import Functions as f

def read_file(filepath, separator) -> pd.DataFrame:
    try:
        entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator, parse_dates=["Date"], date_format="%Y-%m-%d")
    except:
        try:
            entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator)
        except:
            f.log("Failed to open " + filepath)
            entries = pd.DataFrame()
    return entries

def write_file_entries(entries, output, separator):
    outputfile = f.get_full_Path(output)
    if entries.size == 0:
        f.log("No entries to write to file")
    else:
        entries.to_csv(outputfile, sep=separator, index=False, mode="w", header=True,columns=["Date","Type","ID","Name","Account","Quantity","Quantity_Type","Cost","Cost_Type"])
    pass

def write_file_balance(entries, output, separator):
    outputfile = f.get_full_Path(output)
    entries.to_csv(outputfile, sep=separator, index=False, mode="w", header=True)
    pass

def get_cumulativesum(entries):
    entries["RunningTotal"] = entries.groupby(by=["Account","Quantity_Type"])["Quantity"].cumsum(skipna=True)
    return entries



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


def validate_Transaction(data: pd.DataFrame) -> bool:
    data = f.filter_data(data, "Equals", "Type", "Transaction")

    if len(data["Quantity_Type"].unique()) == 1:
        if round(data["Quantity"].sum(), 5) == 0.0:
            return True
        else:
            return False
    else:
        qt_val = {}
        invalid = {}
        for quantity_type in data["Quantity_Type"].unique():
            qt_data = f.filter_data(data, "Equals", "Quantity_Type", quantity_type)
            qt_val[quantity_type] = qt_data["Quantity"].sum()
            if qt_val[quantity_type] != 0.00:
                invalid.update({quantity_type: qt_val[quantity_type]})

        for q_t in invalid:
            qt_data = f.filter_data(data, "Equals", "Cost_Type", q_t)
            for index, row in qt_data.iterrows():
                invalid[q_t] = invalid[q_t] + row["Cost"]
                invalid[row["Quantity_Type"]] = invalid[row["Quantity_Type"]] - row["Quantity"]

        out = True

        for q_t in invalid:
            invalid[q_t] = round(invalid[q_t], 5)
            if invalid[q_t] != 0.0:
                out = False

    return out