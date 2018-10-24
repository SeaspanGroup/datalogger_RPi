import math
import csv
import os
import sys
import syslog
import json
from struct import pack, unpack
from syslogger import Syslogger

class Registers(object):

    def __init__(self, regsFile):
        self.data = []
        self.last = 0
        self.timestamp = 0
        count = 0

        with open(regsFile) as regcsvfile:
            reader = csv.DictReader(regcsvfile)

            for row in reader:
                row['Value'] = 0
                
                address = int(row['Address'])
                number = int(row['Registers'])
                self.last = max(self.last, address+number-1)
                
                self.data.append(dict(row))
                if row['Type'] == 'BIT':
                    count = count+1
                    if count == 1:
                        self.add_first_bit = int(row['Count'])
                #print(count)
            self.length_bit = count
            
            
        #Syslogger.log("imported {} registers from {}".format(len(self.data), regsFile), level=syslog.LOG_DEBUG)

    def get_length_reg(self,count):
        for row in self.data:
            if row['Count'] == count:
                return int(row['Registers'])

    def get_length_bit(self):
        return self.length_bit
    
    def get_add_first_bit(self):
        return self.add_first_bit

    def setData(self,value,count):
        for reg in self.data:
            if reg['Count'] == count:
                reg['Value'] = value
                print('Register: '+ str(reg['Address']+ ' new value ' + str(value)))

    def getData(self): # -> list:
        return self.data

    def allColumns(self): # -> dict:
        result = dict(map(lambda element: (element['Name'], element['Type']), self.data))
        result["ztimestamp"] = "timestamp"
        return result

    @staticmethod
    def convertIEEE754(loWord, hiWord):
        result = unpack('f', pack('<HH', int(loWord), int(hiWord)))[0]
        if math.isnan(result):
            result = 0
        return result
    
    @staticmethod
    def convertText(numbers=[]):
        # print("DEBUG: converting {} to text".format(numbers))
        result = ''
        for number in numbers:
            if number == 0: 
                continue

            hiByte = (number & 0xFF00) >> 8
            loByte = number & 0x00FF

            result += '{}{}'.format(chr(loByte), chr(hiByte))
        
        if (len(result) == 0) or (result == '0') or (result == '0\x00'):
            return 0

        return result

    @staticmethod
    def convertToInt32(loWord, hiWord):
        result = (hiWord << 16) | loWord
        if math.isnan(result):
            result = 0
        return result

    def clear(self):
        for row in self.data:
            row['Value'] = 0

    def debugDump(self, filename):
        #path = os.path.join('/media', 'usb0', filename)
        # for testing :
        path = os.path.join(os.path.dirname(__file__), 'resources', filename)

        if sys.platform == 'linux':
            path = os.path.join(os.path.dirname(__file__), filename)
        
        mode = 'a'
        if os.path.exists(path) is False:
            mode = 'w'

        with open(path, mode) as outfile:
            outfile.write(">\n")
            import pandas as pd
            df = pd.DataFrame(self.data)
            df.to_csv('test_df.csv.gz', compression = 'gzip')
            for row in self.data:
                if row['Value'] == 0.0:
                    continue
                json.dump(row, outfile)
                outfile.write("\n")
            outfile.write("<\n")

if __name__ == '__main__':
     values = (21825, 8280, 28261, 8295, 26995, 26478, 25964, 0, 0, 0)
     text = Registers.convertText(values)
     f = Registers.convertIEEE754(16384,17619)
     f2 = Registers.convertIEEE754(0, 127) #lo, hi
     f3 = Registers.convertIEEE754(57147, 16271) #lo, hi
     print(text)
     print(f)
     print(f2)
     print(f3)
     
     i = Registers.convertToInt32(52429, 16668)
     print(i)
