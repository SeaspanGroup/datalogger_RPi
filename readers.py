from sys import platform
import syslog
from datasources import RealCOMDatasource,ACONISDatasource
from configuration import Configuration, RegisterConfig
from registers import Registers
from syslogger import Syslogger
import time
import datetime
#import paho.mqtt.client as mqtt
import random, threading, json
from datetime import datetime
import uploader as up
#====================================================
## MQTT Settings 
#MQTT_Broker = "iot.eclipse.org"
#MQTT_Port = 1883
#Keep_Alive_Interval = 45
#MQTT_Topic_Humidity = "Home/BedRoom/DHT22/Humidity"
#MQTT_Topic_Temperature = "Home/BedRoom/DHT22/Temperature"

#====================================================

def on_connect(client, userdata, rc):
	if rc != 0:
		pass
		print ("Unable to connect to MQTT Broker...")
	else:
		print ("Connected with MQTT Broker: " + str(MQTT_Broker))

def on_publish(client, userdata, mid):
	pass
		
def on_disconnect(client, userdata, rc):
	if rc !=0:
		pass
		
#mqttc = mqtt.Client()
#mqttc.on_connect = on_connect
#mqttc.on_disconnect = on_disconnect
#mqttc.on_publish = on_publish
#mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))		


def publish_To_Topic(topic, message):
	mqttc.publish(topic,message)
	print ("Published: " + str(message) + " " + "on MQTT Topic: " + str(topic))
	print ("")

class Reader(object):

    def __init__(self, config: RegisterConfig):
        self.registerConfig = config

    def readData(self):
        #registers = self.registerConfig.registers()
        # create registers
        regs = self.registerConfig
        #print(regs)
        # get data from registers :
        #data_registers = regs[0].registers().getData()
        data_registers = regs.registers().getData()

        port = regs.cfgPort()
        alias = regs.cfgAlias()
        #port = regs.cfgTtyName()
        if alias == 'spu':
            # connect to the source :
            datasource = RealCOMDatasource(regs)

            if datasource.connected is False:
                success = False
            else:
                success = True
            # see later if it is mandatory:
            #data_registers.clear()

            for reg in data_registers:
                addr = int(reg['Address'])
                numRegs = int(reg['Registers'])
                valueType = reg['Type']
                
                value = datasource.read(address=addr, numberOfRegisters=numRegs, unit = 1)
                if value is None:
                    reg['Value'] = None
                    Syslogger.log(text="Cannot read data from {}".format(port), level=syslog.LOG_ERR)
                    success = False
                    break # a voir si cest mieux que continue


                if valueType == 'Double':
                    reg['Value'] = Registers.convertIEEE754(value[0], value[1])
                elif valueType == 'Text':
                    reg['Value'] = Registers.convertText(value)
                elif valueType == 'Integer':
                    reg['Value'] = Registers.convertToInt32(value[0], value[1])
                else:
                    reg['Value'] = None

            print('Data read for port {}'.format(port))

            return success

        elif alias == 'ams':
            # Connect to the datasource:
            datasource = ACONISDatasource(regs)
            success, result = datasource.run_server(regs)
            print('Data read for port {}'.format(port))
            return success

if __name__ == '__main__':
    start = time.time()
    Syslogger.log(text='************************* Test Readers *************************************************', level=syslog.LOG_INFO)
    # loading configuration files :
    cfg = Configuration()
    # create registers
    regs = cfg.registerConfigurations
    # get data from registers : 0 is ams
    i = 0
    data_registers = regs[i].registers().getData()
     # if test you want to test ams.
    #i = 1 # if test you want to test spu.
    
    # connect to the source :
    if regs[i].cfgAlias() == 'ams':
    #    datasource = RealCOMDatasource(regs[0])
        datasource = ACONISDatasource(regs[i])
        success, result  = datasource.run_server(regs[i])

    else:
        datasource = RealCOMDatasource(regs[i])
        reader = Reader(regs[i])
        success_read = reader.readData()    
#    print('config load')
    # Read registers :
#    rdrs = Reader(regs[0])
#    print('Reading data...')
#    rdrs.readData()
#    print('reader finished')
#    print(rdrs)

    # send data via mqtt protocol :
#    toggle = 1
    #threading.Timer(3.0, publish_Fake_Sensor_Values_to_MQTT).start()
#    global toggle
    #print(regs[0].registers().data)
#    for row in regs[0].registers().data:
#        if toggle == 1:
#            print(row['Value'])
#            if type(row['Value']) != 'str':
#                Humidity_Fake_Value = float("{0:.2f}".format(int(row['Value'])))

                #Humidity_Data = {}
                #Humidity_Data['Sensor_ID'] = row['Name']
                #Humidity_Data['Date'] = (datetime.today()).strftime("%d-%b-%Y %H:%M:%S:%f")
                #Humidity_Data['Humidity'] = Humidity_Fake_Value
                #humidity_json_data = json.dumps(Humidity_Data)
                ##print ("Publishing fake VAF Value: " + str(row['Value']) + "...")
                #publish_To_Topic (MQTT_Topic_Humidity, humidity_json_data)
                #toggle = 1
    # write registers in txt file :
    #regs[0].registers().debugDump("registers.csv")

#    from writers import SQLiteWriter
#    writer = SQLiteWriter(regs[0])
#    writer.configure()
#    writer.write()
#    writer.export()
    #up.send_file_onshore('/home/pi/Documents/Prototype/test_df.csv.gz')
