#!/usr/bin/env python3

import os
#from sys import platform
import syslog
from configuration import Configuration
import serial
import logging
from struct import pack, unpack
import math
from pymodbus.client.sync import ModbusSerialClient as ModbusClient

class Executor(object):
    @staticmethod
    def requireRoot():
        if os.getuid() > 0:
            raise RuntimeError("Root privileges are required")

    @staticmethod
    def execute(command):
        import subprocess
        try:
            popen = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            oe = popen.communicate()
            syslog.syslog(syslog.LOG_DEBUG, "executed command {}".format(command))
        except Exception as ex:
            syslog.syslog(syslog.LOG_ERR, "failed to execute command {}".format(command))
            return None
        # print ('popen : %s' % (popen))
        # print ('output: %s' % (oe[0]))
        # print ('error : %s' % (oe[1].split(':')))
        return popen.pid

    @staticmethod
    def findPids(processName):
        from subprocess import check_output, CalledProcessError
        try:
            pidlist = list(map(int, check_output(['pidof', processName]).split()))
            return pidlist
        except BaseException:
            return []


class Installer(object):

    verbose = False

    def __init__(self, config):
        if config is None:
            raise RuntimeError('config cannot be NULL')
        self.configuration = config
    
    def openPorts(self):
        Executor.requireRoot()
        if self.verbose is True:
            syslog.syslog(syslog.LOG_INFO, "Opening tty ports")

        for configuration in self.configuration.registerConfigurations:
            syslog.syslog(syslog.LOG_DEBUG, "Opening tty port for box {}".format(configuration))
            print("installing port for box {}".format(configuration.cfgPort()))

            tty = "/dev/{}".format(configuration.cfgPort())
            
            #if os.path.exists(tty) is False:
            tcmd = 'setserial {} port 1'.format(tty)
                #Executor.execute(tcmd)
            os.system(tcmd)
            #else:
            #    print("Cannot find path for {}".format(configuration.cfgPort()))

if __name__ == '__main__':
    cfg = Configuration()
    inst = Installer(cfg)
    inst.openPorts()
    try:
        #instrument.get_all_pattern_variables(0)

        logging.basicConfig()
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)

        client = ModbusClient(method='rtu',port='/dev/ttyUSB0',parity='N', stopbits=1, bytesize=8, baudrate=57600, timeout=3)
        if client.connect():
           print ("Port /dev/ttyUSB0 open")

    except IOError:
        print("Failed")
