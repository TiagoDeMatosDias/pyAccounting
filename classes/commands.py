import pandas as pd


def command_parser(run, config):
    if run["type"] == "IBKR":
        from classes.parser_IBKR import write_Entries
        outputFile = run["output"]
        write_Entries(outputFile, config)
        pass
    elif run["type"] == "n26":
        pass
    elif run["type"] == "YahooFinance":
        pass
    pass

def command_merge(run, config, functions):
    entries = []
    input_1 = run["input_1"]
    input_2 = run["input_2"]
    output = run["output"]
    separator = config["CSV_Separator"]

    import classes.pandas as pandas
    entries_1 = pandas.read_file(input_1,separator )
    entries_2 = pandas.read_file(input_2,separator )


    entries = pd.concat( [entries_1, entries_2])
    entries = entries.sort_values(by="Date", ascending=True).reset_index(drop=True)
    pandas.write_file_entries(entries,output,separator)
    pass



class Commands:

    def run_command(run, config, functions):

        if run["task"] == "parser":
            command_parser(run, config)
        elif run["task"] == "merge":
            command_merge(run, config, functions)
            pass
        elif run["task"] == "benchmark":
            pass
        else:
            print("Other")
