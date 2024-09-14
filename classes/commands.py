from datetime import datetime

import pandas as pd
from decimal import Decimal
from classes.functions import Functions as f
import classes.pandas as pandas
import classes.data as d







class Commands:

    def run_command(run, config):
        if run["task"] == "parser":
            d.command_parser(run, config)
        elif run["task"] == "merge":
            d.command_merge(run, config)
            pass
        elif run["task"] == "benchmark":
            d.command_benchmark(run, config)
            pass
        elif run["task"] == "balance":
            d.command_balance(run, config)
            pass
        elif run["task"] == "runningTotal":
            d.command_runningTotal(run, config)
            pass
        elif run["task"] == "chart":
            d.command_chart(run, config)
            pass
        elif run["task"] == "filter":
            d.command_filter(run, config)
            pass
        elif run["task"] == "validate":
            d.command_validate(run, config)
            pass
        elif run["task"] == "compress":
            d.command_compress(run, config)
            pass
        else:
            f.log("Other Command")
