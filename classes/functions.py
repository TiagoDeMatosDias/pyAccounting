import datetime
import json
import os
import csv
import uuid
import xml
import xml.etree.ElementTree as ET
import typing

import itertools

from decimal import Decimal

class Functions:
    def import_json(filename):

        with open(Functions.get_full_Path(filename), "r") as read_file:
            data = json.load(read_file)
            return data
    pass



    def get_full_Path(relativePath, filename=None):
        if filename==None:
            return os.path.join(os.getcwd(), relativePath)
        else:
            return os.path.join(os.getcwd(), relativePath, filename)
    pass


    def get_ListFilesInDir(folder):
        """Return a list of files only (not folders) within the given directory"""
        return [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]

    def read_XML(file):
        tree = ET.parse(file)
        root = tree.getroot()
        return root
    pass

    def get_XML_Tag_All(XML):
        return [{'tag': elem.tag, 'attrs': elem.attrib} for elem in XML.iter() ]
    pass

    def combine_lists(dict1, dict2):
        combined = {}
        if len(dict1)==0:
            return dict2
        if len(dict2)==0:
            return dict1
        for key in set(list(dict1.keys()) + list(dict2.keys())):
            combined[key] = list(itertools.chain(dict1.get(key, []), dict2.get(key, [])))
        return combined

    def get_priceUpdates(entries):
        return entries.loc[(entries['Type'] == "PriceUpdate")  ]

    def get_transactions(entries):
        return entries.loc[(entries['Type'] == "Transaction")  ]

    def get_benchmark(entries):
        return entries.loc[(entries['Type'] == "Benchmark")  ]

    def get_LatestPrice(PriceChanges, date, Ticker, currency, depth, maxDepth):
        if Ticker == currency:
            return 1

        priceOptions = PriceChanges.loc[(PriceChanges['Date'] <= date) & (PriceChanges['Quantity_Type'] == Ticker)][
            "Cost_Type"].unique()
        if len(priceOptions) == 0:
            return 0

        try:
            price = PriceChanges.loc[(PriceChanges['Date'] <= date) & (PriceChanges['Quantity_Type'] == Ticker) & (
                        PriceChanges['Cost_Type'] == currency)].sort_values(by="Date", ascending=False).iloc[0]['Cost']
        except:
            depth = depth + 1
            if depth < maxDepth:

                for option in priceOptions:
                    price = Functions.get_LatestPrice(PriceChanges, date, option, currency, depth, maxDepth)
                    if price != None:
                        price = price * Functions.get_LatestPrice(PriceChanges, date, Ticker, option, depth, maxDepth)
                        return price
            return 0
        return price




    def log(logData):
        with open("log.txt", "a") as myfile:
            time = str(datetime.datetime.now())
            log = time + ": " + str(logData) + "\n"
            myfile.write(log)
        print( log)

    def get_runParameter(run, parameter):
        try:
            output = run[parameter]
        except:
            output = None

        if output == "":
            output = None
        return output

    def run_filters(data, filters):
        if filters == None:
            return  data

        filtered_data = data
        for filter in filters:
            type = Functions.get_runParameter(filter, "type")
            column = Functions.get_runParameter(filter, "column")
            value = Functions.get_runParameter(filter, "value")
            filtered_data = Functions.filter_data(filtered_data, type, column, value)
        return filtered_data

    def filter_data(data, type, column, value):
        if type == "Min":
            return data.loc[(data[column] >= value)]
        elif type == "Max":
            return data.loc[(data[column] <= value)]
        elif type == "Contains":
            return data.loc[(data[column].str.contains(value)==True)]
        elif type == "Equals":
            return data.loc[(data[column]==value)]
        else:
            return data