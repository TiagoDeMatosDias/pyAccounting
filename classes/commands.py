def command_parser(run, config):
    if run["type"] == "IBKR":
        from classes.parser_IBKR_pd import write_Entries
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
    output = functions.get_full_Path(output)
    separator = config["CSV_Separator"]
    entries = functions.read_CSV(functions,input_1,separator)
    entries = entries + functions.read_CSV(functions,input_2,separator)
    from classes.Entry import Entry
    entries = sorted(entries, key=Entry.sort_priority)
    functions.write_CSV(output, entries, separator)
    print("hello")
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
