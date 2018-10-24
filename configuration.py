from sys import platform
import os
import csv
from registers import Registers
import fnmatch

class Configuration(object):

    # creation of the class with this parameters :
    def __init__(self):
            
            self.registerConfigurations = []
            self.reload()

        # Method reload : load the config.csv file and read the csv to fill the registerConfig class
    def reload(self):
        #load config.csv from resources
        configPath = os.path.join(self.__resourcesDir(), "config.csv")
        if os.path.exists(configPath) is False:
            raise RuntimeError ("config file not found at path: {}".format(configPath))
        
        newRegisterConfigurations = []

        with open(configPath) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                d = dict(row)
                alias = d['alias']

                #read the registers from the respective csv for the alias
                regsFile = os.path.join(self.__resourcesDir(), "{}.csv".format(alias))
                
                if os.path.exists(regsFile) is True:
                    regs = Registers(regsFile)
                    regCfg = RegisterConfig(d, regs)
                    #regCfg.check_tty()
                    newRegisterConfigurations.append(regCfg)
                    #print(regs.getData())

                regCfg = RegisterConfig(d, regs)
                #newRegisterConfigurations.append(regCfg)

        #replace with new config
        self.registerConfigurations = newRegisterConfigurations
	
    
    # Print current config :
    def UportConfig(self):
        result = """#==============================================================#
#   This configuration file was created by Seaspan Ltd #
#==============================================================#
#[Minor] [ttyName] [interface] [mode] [alias]\n"""
        for config in self.registerConfigurations:
            line = "{} \t {} \t {} \t {} \t {} \n".format(config.cfgMinor(),config.cfgPort(), config.cfgInterface(), config.cfgMode(), config.cfgAlias())
            result += line
        return result

    # configure config file path:
    def __resourcesDir(self):
        return os.path.join(os.path.dirname(__file__), 'resources')
    def __logger_dir(self):
        return(os.path.dirname(__file__))
    # create a dictionary from config files.
    def __parse(self, line):
        #print 'parsing line: %s' % (line)
        #if line.startswith('ttymajor'):
        #    value = line.split('=')[1]
        #    self.ttymajor = int(value)
        #    return
        #if line.startswith('calloutmajor'):
        #    value = line.split('=')[1]
        #    self.calloutmajor = int(value)
        #    return

        values = ' '.join(line.split(' ')).split()
        #create dictionary from values
        if len(values) < 10:
            return
        
        #make this a dictionary
        index = int(values[0])
        keys = ['Minor','ttyName','interface','mode','alias']
        d = dict(zip(keys, values[1:10]))
        self.registerConfigurations.append(d)

class RegisterConfig(object):

    def __init__(self, config: dict, registers: Registers):
        self.config = config
        self.regs = registers
        self.connected = True

    def cfgMinor(self) -> int:
        return int(self.config['Minor'])

    def cfgPort(self) -> str:
        return str(self.check_tty())

    def cfgInterface(self) -> int:
        return int(self.config['interface'])

    def cfgMode(self) -> str:
        return str(self.config['mode'])
     
    def cfgAlias(self) -> str:
        return str(self.config['alias'])

    def cfgbytesize(self) -> str:
        return int(self.config['bytesize'])
    
    def cfgbaudrate(self) -> str:
        return int(self.config['baudrate'])
    
    def cfgparity(self) -> str:
        return str(self.config['parity'])
    
    def cfgtimeout(self) -> str:
        return int(self.config['timeout'])
    
    def cfgstopbits(self) -> str:
        return int(self.config['stopbits'])
    
    def registers(self) -> Registers:
        return self.regs

    def connected(self) -> str:
        return self.connected

    def set_connection(self,value):
        self.connected = value
        return self.connected

    #def set_uport_name(self, value):
    #    self.cfgPort = value

    def check_tty(self):
            # look into /dev file and count and get names of the ttyUSB ports :
        os.chdir('/dev')
        output = os.popen("ls | grep {}".format('ttyUSB')).readlines()
        if len(output) != 0: # At least one UPort connected ...
            if len(fnmatch.filter(output,'*' + self.config['ttyName']+'*')) == 0 and len(output) == 2:
                #print('Correct config and tty names for {}'.format(self.config['ttyName']))
                return output[-1][0:-1]
            else:
               # print('No match for {}. Possible names for Uports : {}'.format(self.config['ttyName'],output))
                return self.config['ttyName']
                #print('New Name : '+ output[-1][0:-1])
        else: # No Uport connected.. Problem
            print('No Uport connected')

if __name__ == '__main__':
    cfg = Configuration()
    print(cfg.UportConfig())

    #cfg.reload()
