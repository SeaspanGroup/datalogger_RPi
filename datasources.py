""" datasources
This module implements data sources that are being used by the 'Reader'.
"""
import time
import random
from syslogger import Syslogger
import syslog
import pymodbus
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import serial
import struct
from struct import *
from pymodbus.compat import int2byte, byte2int, IS_PYTHON3
import math
import pandas as pd
# Invert byte ( LO and HI )
def swapLoHi(n):
    lo = n & 0x00FF
    hi = (n & 0xFF00) >> 8
    return  lo << 8 | hi

def __generate_crc16_table():
    """ Generates a crc16 lookup table
    .. note:: This will only be generated once
    """
    result = []
    for byte in range(256):
        crc = 0x0000
        for _ in range(8):
            if (byte ^ crc) & 0x0001:
                crc = (crc >> 1) ^ 0xa001
            else: crc >>= 1
            byte >>= 1
        result.append(crc)
    return result

__crc16_table = __generate_crc16_table()

def computeCRC(data):
    """ Computes a crc16 on the passed in string. For modbus,
    this is only used on the binary serial protocols (in this
    case RTU).
    The difference between modbus's crc16 and a normal crc16
    is that modbus starts the crc value out at 0xffff.
    :param data: The data to create a crc16 of
    :returns: The calculated CRC
    """
    crc = 0xffff
    for a in data:
        idx = __crc16_table[(crc ^ byte2int(a)) & 0xff]
        crc = ((crc >> 8) & 0xff) ^ idx
    swapped = ((crc << 8) & 0xff00) | ((crc >> 8) & 0x00ff)
    return swapped

def convertIEEE754(loWord, hiWord):
    result = unpack('f', pack('<HH', int(loWord), int(hiWord)))[0]
    if math.isnan(result):
        result = 0
    return result

def convertIEEE754todouble(w1, w2,w3,w4):
    result = unpack('d', pack('<HHHH', int(w1),int(w2),int(w3), int(w4)))[0]
    if math.isnan(result):
        result = 0
    return result
class _Datasource(object):

    connected = False

    def __init__(self):
        pass

    def read(self, address, numberOfRegisters=2):
        pass

# SPU Datasource as master
class RealCOMDatasource(_Datasource):
    "A datasource for that uses pymodbus to connect to a serial port using modbus."

    def __init__(self, regs):
        tty = '/dev/'+'%s' % (regs.cfgPort())
        Syslogger.log(text="connecting to port: {}".format(tty), level=syslog.LOG_DEBUG)
        try:
            self.instrument = ModbusClient(method=regs.cfgMode(), port=tty, parity=regs.cfgparity(), stopbits=regs.cfgstopbits(), bytesize=regs.cfgbytesize(), baudrate=regs.cfgbaudrate(), timeout=regs.cfgtimeout())
            self.connected = True
            print('connected to {}'.format(tty))
            #client = ModbusClient(method='rtu',port=tty,parity='N',stopbits=1,bytesize=8,baudrate=57600,timeout=3)
            #result = client.read_holding_registers(address=88, count=2,unit=1)
            #print(result.registers)

        except BaseException as ex:
            #self.connected = False
            Syslogger.log(text="connection to port {} failed with {}".format(tty, ex), level=syslog.LOG_ERR)
            
    def read(self, address: int, numberOfRegisters: int, unit: int): 
        try:
            result = self.instrument.read_holding_registers(address=address, count=numberOfRegisters,unit=1)
            #print(result.registers)
            #Syslogger.log(text="1st: reg:{}, res:{}".format(address, result.registers), level=syslog.LOG_DEBUG)
            return result.registers
        #except (ValueError, TypeError, IOError) as err:
        #    Syslogger.log(text="1st read of register {} failed with: {}".format(address, err), level=syslog.LOG_ERR)
        #except BaseException as ex:
        #    Syslogger.log(text="1st read of register {} failed with undefined error: {}".format(ex, err), level=syslog.LOG_ERR)
        except BaseException as ex:
            self.connected = False
            Syslogger.log(text="Read data from port SPU failed with {}".format( ex), level=syslog.LOG_ERR)
            
        #retrying - this could be implemented a bit nicer but I'd like to know if it works first
        time.sleep(1.0)

# AMS Datasource : read as a slave
class ACONISDatasource(_Datasource):
    "A datasource which listen ports to receive modbus packets."

    def __init__(self, regs):
        tty = '/dev/'+ '%s' % (regs.cfgPort())
        Syslogger.log(text="connecting to port: {}".format(tty), level=syslog.LOG_DEBUG)
        try:
            ser = serial.Serial()
            ser.baudrate = regs.cfgbaudrate() # To change into the config file
            ser.bytesize = regs.cfgbytesize()
            ser.stopbits = regs.cfgstopbits()
            ser.timeout = regs.cfgtimeout()
            parity = regs.cfgparity()
            ser.port =tty
            self.ser = ser
            self.read_bytes = ''
        except BaseException as ex:
            Syslogger.log(text="Initialisation of datasource failed : {}".format(tty, ex), level=syslog.LOG_ERR)
    
    def run_server(self,regs):
        try:
            ser = self.ser
            ser.open()
            self.connected = True
            print('connected to {}'.format(self.ser.port))
            # compute theoritical length of the request :
            # Get data from registers :
            data_registers = regs.registers().getData()
            len_bits = regs.registers().get_length_bit()
            add_bits = regs.registers().get_add_first_bit()
            len_theo = 0
            for row in data_registers:
                len_theo = len_theo + int(row['Registers'])
                self.len_theo = len_theo
            # connect to master and get data :
                count = 0
            while self.connected: 
                # Read bytes
                self.read_bytes = self.ser.read(size = 1026)
                print(self.read_bytes)
                if len(self.read_bytes)>=self.len_theo*2+9:
                    #print('Read the entire request:'+str(self.read_bytes[0]))

                    # get the function code
                    (function_code, ) = struct.unpack(">B", self.read_bytes[1:2])
                    print('function_code : '+str(function_code))
                    # get the first register address :
                    (reg_add,) = struct.unpack(">H", self.read_bytes[2:4])
                    print('first_reg_add : '+str(struct.unpack(">H", self.read_bytes[2:4])))
                    #print('reg_add : '+str(reg_add))
                    # get the number of hi+lo value :
                    (nb_value,) = struct.unpack(">H", self.read_bytes[4:6])
                    print('nb_value : '+str(nb_value))
                    if nb_value == self.len_theo:
                        print('############# New request received !')
                        c1 = 7
                        c2 = 11
                        count = 0
                        data = []
                        add_reg = []
                        reg_add = 0
                    
                        while count < nb_value:
                            print('###################### new decompte: '+ str(reg_add))
                            reg_num = regs.registers().get_length_reg(str(reg_add))
                            print('nombre de reg: ' + str(int(reg_num)))
                            if count!=0:
                                c2 = c2 + int(reg_num)*2

                            if int(reg_num) == 2:
                                if reg_add == add_bits:
                                    val_add = struct.unpack(">HH", self.read_bytes[c1:c2])
                                    val_conv = "{0:b}".format(val_add[1])
                                    while len(val_conv) <len_bits:
                                        val_conv = '0'+val_conv
                                    print('test:'+str(val_conv))

                                    for new_count in range(0,len_bits,1):
                                        print('###################### new sub decompte: '+ str(reg_add))
                                        #print('test: '+str(val_conv[new_count]+' : '+str(reg_add)))
                                        regs.registers().setData(val_conv[new_count],str(reg_add))
                                        reg_add = reg_add + 1
                                    
                                    # recount at the end of the for loop :
                                    reg_add = reg_add - 1
                                    val_conv = val_conv[new_count]

                                else:
                                    val_add = struct.unpack(">HH", self.read_bytes[c1:c2])
                                    #print('It is here :' +str(val_add[1]))
                                    val_conv = convertIEEE754(val_add[0],val_add[1])

                            elif int(reg_num) == 4:
                                val_add = struct.unpack(">HHHH", self.read_bytes[c1:c2])
                                #print(read_bytes[c1:c2])
                                val_conv = convertIEEE754todouble(val_add[0], val_add[1],val_add[2],val_add[3])
                                

                            else: 
                                Syslogger.log(text="Number of registers does not match", level=syslog.LOG_ERR)

                            regs.registers().setData(val_conv,str(reg_add))
                            reg_add = reg_add + 1   
                            count = count + int(reg_num)
                            
                            c1 = c2
                                #print('count : ' + str(count))
                            #print(str(c1) + "," + str(c2))

                            #time = 2 # 5 seconds delay
                        my_bytes = self.create_answer()
                        print('Ready to send... '+ str(my_bytes) )
                        ser.write(my_bytes)
                        print('answer sent !')
                        #print(ams.getData())
                        return True, regs
                    else:
                        Syslogger.log(text='Length of data received not match', level=syslog.LOG_ERR)
                elif count == 4: # not to block the other thread
                    Syslogger.log(text="No data received from {} with error : Break the infinite loop".format(self.ser.port), level=syslog.LOG_ERR)
                    return False, None
                    break
                else:
                    count +=1

        except BaseException as ex:
            self.connected = False
            Syslogger.log(text="Running the server {} failed with {}".format(self.ser.port, ex), level=syslog.LOG_ERR)
    
    def create_answer(self):
        try :
            print('Creating the answer')
            # create the answer:
            my_bytes = bytearray()
            my_bytes.append(self.read_bytes[0])
            my_bytes.append(self.read_bytes[1])
            my_bytes.append(self.read_bytes[2])
            my_bytes.append(self.read_bytes[3])
            my_bytes.append(self.read_bytes[4])
            my_bytes.append(self.read_bytes[5])

            crc = struct.pack(">H", computeCRC(my_bytes))
            my_bytes.append(crc[0])
            my_bytes.append(crc[1])
            #print(my_bytes)
            print('answer created')
            #print(self.my_bytes)
            return my_bytes

        except BaseException as ex:
            Syslogger.log(text="Creating ADU for port {} failed with {}".format(self.ser.port, ex), level=syslog.LOG_ERR)


