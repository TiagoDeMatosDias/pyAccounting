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
        with open(filename, "r") as read_file:
            data = json.load(read_file)
            return data
    pass

    def get_working_Directory(self):
        return os.getcwd()
    pass

    def get_full_Path(relativePath, filename=None):
        if filename==None:
            return os.path.join(os.getcwd(), relativePath)
        else:
            return os.path.join(os.getcwd(), relativePath, filename)
    pass



    def convert_Decimal(value)-> Decimal:
        try:
            return Decimal(value)
        except:
            return Decimal(0.00)
        pass



    def get_ListFilesInDir(folder):
        return [os.path.join(folder, f) for f in os.listdir(folder)]
    pass

    def read_XML(file):
        tree = ET.parse(file)
        root = tree.getroot()
        return root
    pass


    def get_XML_Tag_All(XML):
        return [{'tag': elem.tag, 'attrs': elem.attrib} for elem in XML.iter() ]
    pass

    def get_XML_Tag(XML, tag):
        return [{'tag': elem.tag, 'attrs': elem.attrib} for elem in XML.iter() if elem.tag == tag]
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
        return entries.loc[(entries['Type'] <= "PriceUpdate")  ]

    def get_price(priceUpdates, ticker, currency, depth, maxDepth, latest = None):
        prices = priceUpdates.loc[ (priceUpdates['Quantity_Type'] == ticker) ]
        if latest != None:
            prices = prices.loc[prices['Date'] <= latest]
        priceOptions = prices["Cost_Type"].unique()

        price = None
        depth = depth + 1

        if ticker == currency:
            return 1

        for typer in priceOptions:
            if typer == currency:
                prices = prices.loc[prices['Cost_Type'] == currency].sort_values(by="Date", ascending=False)
                return prices["Cost"].iloc[0]

        if price == None:
            if depth <= maxDepth:
                for option in priceOptions:
                    price = Functions.get_price(priceUpdates, option, currency, depth, maxDepth, latest)
                    if price != None:
                        price = Decimal( price ) * Decimal( Functions.get_price(priceUpdates, ticker, option, depth, maxDepth, latest) )
                        return price
        return 0

    def generate_unique_uuid():
       return str(uuid.uuid4())

    def log(logData):
        with open("log.txt", "a") as myfile:
            time = str(datetime.datetime.now())
            log = time + ": " + str(logData)
            myfile.write(log)
        print( log)