import json
import os
import csv
import xml
import xml.etree.ElementTree as ET
import typing

import itertools

from classes.Entry import Entry as entry
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

    def write_CSV(filename, list, separator, header=True, overwrite=True):
        if overwrite:
            type = "w"
        else:
            type = "a"
        with open(filename, type) as f:
            for entry in list:
                if header:
                    f.write(f"{entry.headers_CSV(separator)}\n")
                    header = False
                f.write(f"{entry.write_CSV(separator)}\n")
    pass

    def convert_Decimal(value)-> Decimal:
        try:
            return Decimal(value)
        except:
            return Decimal(0.00)
        pass

    def read_CSV(self,filename, separator, header=True):
        entries = []
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            stringList = [row for row in reader]
            if header:
                stringList.pop(0)
            for row in stringList:
                fullline = ""
                for line in row:
                    fullline = fullline + line
                parameters = fullline.split(separator)
                e = entry(parameters[0], parameters[1], parameters[2], parameters[3], parameters[4], self.convert_Decimal(parameters[5]), parameters[6], self.convert_Decimal(parameters[7]), parameters[8])
                entries.append(e)
        return entries
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