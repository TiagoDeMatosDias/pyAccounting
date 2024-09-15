from decimal import Decimal

import pandas as pd
from classes.functions import Functions as f


def read_file(filepath: str, separator: str) -> pd.DataFrame:
    """
    Reads a CSV file into a DataFrame with optional date parsing and type conversion.

    Parameters:
    - filepath: The path to the CSV file.
    - separator: The separator used in the CSV file (e.g., ',' or ';').

    Returns:
    - A pandas DataFrame containing the parsed file, or an empty DataFrame if an error occurs.
    """
    try:
        # Attempt to read the file with date parsing
        entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator, parse_dates=["Date"], date_format="%Y-%m-%d")
    except Exception as e:
        f.log(f"Failed to parse dates from file {filepath}: {e}")

        # Retry without date parsing if the first attempt fails
        try:
            entries = pd.read_csv(filepath_or_buffer=filepath, sep=separator)
        except Exception as e:
            f.log(f"Failed to read file {filepath}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame if both attempts fail

    # Check if the DataFrame is not empty
    if not entries.empty:
        # Convert "Quantity" and "Cost" columns to Decimal if they exist
        for column in ["Quantity", "Cost"]:
            if column in entries.columns:
                try:
                    entries[column] = entries[column].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)
                except Exception as e:
                    f.log(f"Failed to convert column {column} to Decimal in file {filepath}: {e}")

    return entries


def write_file(data, output, separator):
    """
    Writes a DataFrame to a CSV file with specified columns if they exist.

    Parameters:
    - data: The DataFrame to be written to the CSV file.
    - output: The output file path for the CSV file.
    - separator: The separator to use in the CSV file (e.g., ',' or ';').
    """

    # Get the full path for the output file
    outputfile = f.get_full_Path(output)
    f.log(f"Output file path resolved: {outputfile}")

    # Set of columns to check before writing
    columns_to_check = {"Date", "Type", "ID", "Name", "Account", "Quantity", "Quantity_Type", "Cost", "Cost_Type"}

    # Check if all the specified columns exist in the DataFrame
    if columns_exist(data, columns_to_check):
        # If columns exist, write the CSV with only these columns in the specified order
        f.log(f"All required columns found. Writing file with specified columns: {columns_to_check}")
        data.to_csv(outputfile, sep=separator, index=False, mode="w", header=True,
                    columns=["Date", "Type", "ID", "Name", "Account", "Quantity", "Quantity_Type", "Cost", "Cost_Type"])
    else:
        # If columns don't exist, write the entire DataFrame as-is
        f.log("Not all required columns are present. Writing entire DataFrame.")
        data.to_csv(outputfile, sep=separator, index=False, mode="w", header=True)

    f.log(f"File written successfully to: {outputfile}")


def columns_exist(data, columns_to_check):
    """
    Checks if all columns in 'columns_to_check' exist in the DataFrame.

    Parameters:
    - data: The DataFrame in which to check for the columns.
    - columns_to_check: A set of column names to check for in the DataFrame.

    Returns:
    - True if all columns exist, False otherwise.
    """

    # Log the columns being checked and the columns available in the DataFrame
    f.log(f"Checking if the following columns exist: {columns_to_check}")
    f.log(f"Available columns in the DataFrame: {set(data.columns)}")

    # Check if the columns_to_check are a subset of the DataFrame's columns
    if columns_to_check.issubset(data.columns):
        f.log("All required columns exist.")
        return True

    # Log if some columns are missing
    f.log("Some required columns are missing.")
    return False



def get_cumulativesum(entries):
    # Convert 'Quantity' to float for computation
    entries['Quantity_float'] = entries['Quantity'].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    # Calculate cumulative sum with float values
    entries['RunningTotal_float'] = entries.groupby(by=["Account", "Quantity_Type"])['Quantity_float'].cumsum()

    # Optionally convert the RunningTotal back to Decimal if required
    entries['RunningTotal'] = entries['RunningTotal_float'].apply(lambda x: Decimal(str(x)))

    # Drop temporary columns
    entries = entries.drop(columns=['Quantity_float', 'RunningTotal_float'])

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