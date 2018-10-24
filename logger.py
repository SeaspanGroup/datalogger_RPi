#! /usr/bin/env python3

import os
import sys
import platform
import signal
import datetime
import time
from threading import Thread, Lock
from configuration import Configuration, RegisterConfig
from readers import Reader
from timer import RepeatingTimer
from writers import SQLiteWriter
import syslog
from syslogger import Syslogger
from uploader import Uploader
import uploader
import fnmatch
from dateutil import relativedelta
import config_onboard

class Logger(object):

    def __init__(self):
        text = '###################### Initialization of the Seaspan Ltd. Data Logger ######################'
        Syslogger.log(text=text, level=syslog.LOG_INFO)

        self.configuration = Configuration()
        self.logger_dir = os.path.dirname(os.path.realpath('logger.py'))
        self.finish = False
        self.lock = Lock()

        #the currently running threads we use for logging per ip
        self.runningLoggerThreads = {}
        self.runningUploadThread = None

        #version
        self.version = "1.2.3"

        #signal handling
        signal.signal(signal.SIGTERM, self.deinit)
        signal.signal(signal.SIGINT, self.deinit)
        self.check_con = 0
        # Check Uport physical connection:
        self.check_time = datetime.datetime.now()
        self.check_uport()
        self.check_con = 1


        # check USB stick connection :
        #self.check_usb_stick()
    
    # check if USB stick : TO DO 
    #def check_usb_stick(self):

    # check if UPort are connected :
    def check_uport(self):
        if self.check_con == 0:
            text = '###################### Checks ######################'
            Syslogger.log(text=text, level=syslog.LOG_INFO)
        
        for regConfig in self.configuration.registerConfigurations:
            os.chdir('/dev')
            output1 = os.popen("ls | grep {}".format(regConfig.cfgPort())).readlines()

            if len(output1)!=0:
            #fnmatch.fnmatch(output,'*{}*'.format(regConfig.cfgPort())):
                #Syslogger.log(text='Port {} is connected'.format(regConfig.cfgPort()), level=syslog.LOG_INFO)
                if regConfig.connected == False: # usb UPORT has been reconnected :
                    #self.configuration = Configuration()
                    #print(regConfig.cfgPort())
                    self.check_con = 0
                    Syslogger.log(text='################################ UPort has been reconnected ########################################', level=syslog.LOG_INFO)

                output = os.popen("setserial /dev/{}".format(regConfig.cfgPort())).readlines()
                line = output[0]
                if fnmatch.fnmatch(line,'*0x0001*') and self.check_con == 0:
                    Syslogger.log(text='Port {} is connected with correct serial set-up: {}'.format(regConfig.cfgPort(), line), level=syslog.LOG_INFO)
                elif fnmatch.fnmatch(line,'*0x0000*') and self.check_con == 0:
                    Syslogger.log(text='Port {} is connected with wrong serial set-up: {}'.format(regConfig.cfgPort(), line), level=syslog.LOG_INFO)
                    os.popen('setserial /dev/{} port 1'.format(regConfig.cfgPort()))
                regConfig.connected = True
                
                #print(regConfig.connected)

            else:
                Syslogger.log(text='Warning port {} is disconnected @ {} : email send every {} '.format(regConfig.cfgPort(),str(self.check_time),'24 hrs'), level=syslog.LOG_INFO)
                text = 'RPi speaking : Warning port {} is disconnected since {}'.format(regConfig.cfgPort(), str(self.check_time))
                if regConfig.connected == True:
                    uploader.send_warning_onshore(text)

                #if regConfig.connected == False and self.check_time <= datetime.datetime.now() - relativedelta.relativedelta(days = 1) :
                #    uploader.send_warning_onshore(text)
                #    self.check_time = datetime.datetime.now()
                regConfig.connected = False
                #print(regConfig.connected)
        
        os.chdir(self.logger_dir)

    def deinit(self, signum, frame):
        Syslogger.log(text='stopping logger with signal {}'.format(signum), level=syslog.LOG_INFO)
        self.stopRunningThreads(self.runningLoggerThreads)
        self.stopRunningThreads([self.runningUploadThread])
        self.finish = True

    def start(self):
        text = '###################### Running Data Logger v{} pid: {} on python {} ######################'.format(self.version, os.getpid(), platform.python_version())
        Syslogger.log(text=text, level=syslog.LOG_INFO)

        timers = []

        loggingInterval = config_onboard.loggingInterval
        uploadingInterval = config_onboard.uploadingInterval

        timer = RepeatingTimer(loggingInterval, self._onMinuteTimer)
        timer.daemon = True
        timers.append(timer)

        #additional 1/4 hourly timer:
        timer2 = RepeatingTimer(uploadingInterval, self._onQuarterHourlyTimer)
        timer2.daemon = True
        timers.append(timer2)

        for t in timers:
            t.start()
        
        while self.finish is False:
            time.sleep(0.5)

        for t in timers:    
            t.cancel()
        
        self.stopRunningThreads(self.runningLoggerThreads)
        self.stopRunningThreads([self.runningUploadThread])

    def stopRunningThreads(self, threads: dict): #todo: parameter of threads dictionary
        for runningThread in threads:
            if runningThread.is_alive() is True:
                runningThread._stop()
        Syslogger.log(text='stopping running threads', level=syslog.LOG_INFO)

    def _onMinuteTimer(self):
        self.__startMinuteLogging()

    def _onQuarterHourlyTimer(self):
        self.check_uport()
        self.__startUploadThread()

    def __startMinuteLogging(self):

        self.configuration.reload()
        if not self.configuration.registerConfigurations:
            Syslogger.log(text="No configuration found; nothing to connect to...", level=syslog.LOG_INFO)
            return

        #create reader for each ip address
        threadPool = []

        timeindex = int(time.time())

        for regConfig in self.configuration.registerConfigurations:
            
            #set the timeindex of each register config to the same value - helps with joining tables later
            regConfig.registers().timestamp = timeindex

            if regConfig.cfgPort() in self.runningLoggerThreads:
                runningLoggerThread = self.runningLoggerThreads[regConfig.cfgPort()]
                if runningLoggerThread.is_alive() is True:
                    Syslogger.log('previous logger threads are still running', level=syslog.LOG_ERR)
                    continue
                else:
                    del self.runningLoggerThreads[regConfig.cfgPort()]

            newThread = Thread(target=self.__dataLogger, args=[regConfig])
            newThread.daemon = True
            threadPool.append(newThread)

            #hang on to the new thread until it's done
            #self.runningLoggerThreads[regConfig.cfgPort()] = newThread

        for newThread in threadPool:
            newThread.start()

        # for t in threads:
        #     t.join()

    def __startUploadThread(self):
        if not self.configuration.registerConfigurations:
            Syslogger.log(text="No configuration found; nothing to connect to...", level=syslog.LOG_INFO)
            return
        
        threadPool = []

        if self.runningUploadThread is not None and self.runningUploadThread.is_alive() is True:
            
            Syslogger.log('previous upload thread is still running', level=syslog.LOG_ERR)
            return
        else:
            if uploader.test_ping_IP() == True:
                self.runningUploadThread = None

                newThread = Thread(target=self.__upload)
                newThread.daemon = True
                threadPool.append(newThread)

                self.runningUploadThread = newThread

                for newThread in threadPool:
                    newThread.start()
            else:
                Syslogger.log('Do not start thread: Server unreachable', level=syslog.LOG_ERR)

    def __dataLogger(self, registerConfig: RegisterConfig):
        start = time.time()
        Syslogger.log('{}: START logData for ip: {}'.format(str(datetime.datetime.now()), registerConfig.cfgPort()))

        self.lock.acquire()
        self.check_uport()

        reader = Reader(registerConfig)
        self.success_read = reader.readData()
        
        if self.success_read:
            writer = SQLiteWriter(registerConfig)
            writer.configure()
            writer.write()
        
        self.lock.release()

        stop = time.time()
        Syslogger.log('{}: DONE logData for ip: {} in {}s'.format(str(datetime.datetime.now()), registerConfig.cfgPort(), stop-start))
        
    def __upload(self):
        text = '###################### Uploading data ######################'
        Syslogger.log(text=text, level=syslog.LOG_INFO)
        start = time.time()
        Uploader('','','','').connect_server
        if Uploader('','','','').connect_server:
            try:
                for regConfig in self.configuration.registerConfigurations:
                    
                    self.lock.acquire()
                    writer = SQLiteWriter(regConfig)
                    success, localFilePath, fileName, lastIdentifier, lastTimestamp = writer.export()
                    print('Export of {} DONE'.format(fileName))
                        
                    self.lock.release()

                    if success is True:
                        uploader = Uploader(localFilePath, fileName, lastIdentifier, lastTimestamp)
                        if uploader.connect_server:
                            uploadSuccess = uploader.send_file_onshore(regConfig.cfgAlias(),os.path.join('/media', 'pi', config_onboard.USB_key,'itss_datacollect.zip'))
                            print('Files sent !')
                            os.chdir(os.path.join('/media', 'pi/'+ config_onboard.USB_key))
                            os.popen('rm {}'.format(fileName))
                            os.popen('rm itss_datacollect.zip')

                            if uploadSuccess is True:
                                if writer.purge(lastIdentifier, lastTimestamp) is True:
                                    Syslogger.log("PURGED until identifier {} and timestamp {}".format(lastIdentifier, lastTimestamp), level=syslog.LOG_INFO)
                        else:
                            Syslogger.log("Problem connection server", level=syslog.LOG_INFO)
                    
            except BaseException as ex:
                Syslogger.log(text="error uploading: {}".format(ex), level=syslog.LOG_ERR)
                self.lock.release()
                files = os.listdir(os.path.join('/media', 'pi/' + config_onboard.USB_key))
                #if fnmatch.filter(files, '*.zip'):
                #    os.chdir(os.path.join('/media', 'pi/ADATA UFD'))
                #    os.popen('rm itss_datacollect.zip')
                
            finally:
                stop = time.time()
                Syslogger.log('{}: DONE uploading in {}s'.format(str(datetime.datetime.now()), stop-start))
        else:
            stop = time.time()
            Syslogger.log('{}: POSTPONE uploading until server connection comes back'.format(str(datetime.datetime.now())))
            
if __name__ == '__main__':
    logger = Logger()
    logger.start()
