import sys
import os
import json
import sqlite3
import syslog
import csv
from syslogger import Syslogger
from configuration import RegisterConfig
import pandas as pd
import xml.etree.ElementTree as et
import datetime
import calendar
import config_onboard

def change_format(var, col_name, source):
    t, val = var
    return col_name, source, t, val

def change_format2(df,col_name, source):
    
    df_ch = df[['ztimestamp', col_name]].apply(lambda x : change_format(x, col_name, source), axis = 1)

    df_ch2 = pd.DataFrame(columns = {'FIELDKEY', 'SOURCE', 'TIMESTAMP', 'VALUE'})

    df_ch2['FIELDKEY'] = df_ch.apply(lambda x: x[0])
    df_ch2['SOURCE'] = df_ch.apply(lambda x: x[1])
    df_ch2['TIMESTAMP'] = df_ch.apply(lambda x: x[2])
    df_ch2['VALUE'] = df_ch.apply(lambda x: x[3])
    return df_ch2

def field_keys(): # create from the ressource folder, the dict for matching the field names>
    
    field_keys = pd.read_excel(os.path.join('/home/pi/Documents/Datalogger/resources', 'field_keys.xlsx'), sheet_name = 'SSML MODBUS Master')
    field_keys = field_keys.dropna(subset = ['Seaspan'])

    field_keys = field_keys[['Unnamed: 3', 'Seaspan']]
    field_keys.columns = field_keys.iloc[0]
    field_keys = field_keys.set_index('Name').to_dict()
    return field_keys

class Writer(object):

    def configure(self):
        pass
    
    def write(self):
        pass

class SQLiteWriter(Writer):

    def __init__(self, configuration: RegisterConfig):
        self.registerConfig = configuration
    
    def _connect(self, wal: bool=True):
        try:
            if os.path.ismount('/media/pi/'+config_onboard.USB_key) is False:
                Syslogger.log("USB device not mounted", level=syslog.LOG_WARNING)
                if sys.platform != 'darwin':
                    return None, None
                else:
                    dbPath = os.path.join(os.path.dirname(__file__), 'SSML.db')
            else:
                #dbPath = os.path.join('/media', 'usb0', 'hermont.db')
                dbPath = os.path.join('/media', 'pi', config_onboard.USB_key, 'SSML.db')
            #print(os.path.exists(dbPath))
            #create a new connection
            #print(dbPath)
            connection = sqlite3.connect(dbPath, isolation_level=None)

            #if the caller wishes to enable WAL, do it now
            if wal is True:
                connection.execute('PRAGMA journal_mode=WAL')
                #Syslogger.log("WAL mode enabled", level=syslog.LOG_DEBUG)
                # connection.commit() #not sure this is required; can't hurt though

            cursor = connection.cursor()
            return connection, cursor
        except BaseException as ex:
            Syslogger.log(text="connect failed with: {}".format(ex), level=syslog.LOG_ERR)
            connection.close()

        return None, None

    def _disconnect(self, cursor: sqlite3.Cursor):
        try:
            cursor.connection.close()
        except BaseException as ex:
            Syslogger.log(text="disconnect failed with: {}".format(ex), level=syslog.LOG_ERR)

    ## public api

    def configure(self):
        connection, cursor = self._connect()
        
        if cursor is None:
            return False

        try:
            sql = self._sqlCreateTable()
            cursor.executescript(sql)
            connection.commit()

            columns = self.registerConfig.registers().allColumns()
            sql = self._sqlUpdateColumns(columns, cursor) #this doesn't commit the transaction
            
            if sql is not None:
                cursor.executescript(sql)
                connection.commit()

        except sqlite3.OperationalError as err:
            Syslogger.log(text="configure table failed with: {}".format(err.message), level=syslog.LOG_ERR)
            return False
        except BaseException as ex:
            Syslogger.log(text="configure table failed with: {}".format(ex), level=syslog.LOG_ERR)
            return False
        finally:
            self._disconnect(cursor)
        
        return True

    def write(self):
        try:
            sql, values = self._sqlInsertValues()
            if sql is None:
                return
        except BaseException as ex:
            Syslogger.log(text="write table failed with: {}".format(ex), level=syslog.LOG_ERR)
            return

        connection, cursor = self._connect()
        if cursor is None:
            Syslogger.log(text="no cursor to DB!", level=syslog.LOG_ERR)
            return

        try:
            if values is None:
                cursor.executescript(sql)
            else:
                cursor.execute(sql, values)
            connection.commit()
        except (ValueError, IOError, sqlite3.OperationalError) as err:
            Syslogger.log(text="sql {} write failed with: {}".format(sql, err), level=syslog.LOG_ERR)
        except BaseException as ex:
            Syslogger.log(text="sql write failed with: {}".format(ex), level=syslog.LOG_ERR)
        finally:
            self._disconnect(cursor)

    def export(self, baseFileName="export.csv"):
        connection, cursor = self._connect()
        if cursor is None:
            Syslogger.log(text="no cursor to DB!", level=syslog.LOG_ERR)
            return (False, None, None, None, None)
        
        connection.row_factory = sqlite3.Row
        tableName = self.registerConfig.cfgAlias()
        try:
            sql = "SELECT * FROM {}".format(tableName)
            cursor.execute(sql)
        except:
            return (False, None, None, None, None)

        if os.path.ismount('/media/pi/'+config_onboard.USB_key) is False:
            Syslogger.log("USB device not mounted", level=syslog.LOG_WARNING)

        fileName = "{}_{}".format(tableName, baseFileName)
        csvFileName = os.path.join('/media', 'pi/'+config_onboard.USB_key, fileName)
        fileName_xml = 'modbus_data_'+datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")+'.xml'
        path_filename_xml = os.path.join('/media', 'pi/',config_onboard.USB_key, fileName_xml)
        lastIdentifier = 0
        lastTimestamp = 0

        with open(csvFileName, mode='w') as csvFile:
            headers = []

            try:
                writer = csv.writer(csvFile, delimiter=",", quotechar="\"", quoting=csv.QUOTE_NONNUMERIC)
                for row in cursor:
                    if not headers:
                        headers = [description[0] for description in cursor.description]
                        writer.writerow(headers)
                    writer.writerow(row)
                    identifierIndex = headers.index("identifier")
                    timestampIndex = headers.index("ztimestamp")
                    lastIdentifier = row[identifierIndex]
                    lastTimestamp = row[timestampIndex]
                #SUCCESS!
                
            except:
                print(fileName +' problem creating...' )
                return (False, None, None, None, None)
            finally:
                self._disconnect(cursor)
        try:
            s = os.path.getsize(csvFileName)
            
            if s>0:
                Syslogger.log('Creating df from csv size {}'.format(s))
                df = pd.read_csv(csvFileName)
                
                Syslogger.log('Creating df.csv.gzip... ')
                df.to_csv(csvFileName+'.gz', compression = 'gzip')
                Syslogger.log('Creating dict from csv... ')
                df.to_dict(orient= 'dict')
                        
                df_ch = pd.DataFrame(columns = {'FIELDKEY', 'SOURCE', 'TIMESTAMP', 'VALUE'})
                if self.registerConfig.cfgAlias() == 'ams':
                    source_name = 'ACONIS'
                    df_ch['SOURCE'] = 'ACONIS'
                else:
                    source_name = 'TTSENSE'
                    df_ch['SOURCE'] = 'TTSENSE'

                df_ch3 = pd.DataFrame(columns = {'FIELDKEY', 'SOURCE', 'TIMESTAMP', 'VALUE'})
                for i in df.columns:
                    if i != 'Unnamed: 0' or i != 'identifier' or i != 'ztimestamp':
                        df_ch3 = df_ch3.append(change_format2(df, i, source_name))

                df_ch3 = df_ch3.reset_index(drop = True)
                field_key = field_keys()
                df_ch3['FIELDKEY'] = df_ch3['FIELDKEY'].map(field_key['Field key'])
                df_ch3 = df_ch3.dropna()

                # transform into a dict:
                df_ch4 = df_ch3.T.to_dict(orient = 'dict')
                df = pd.DataFrame(df_ch4).T
				
                #root = et.Element('?xml version="1.0" ?')
                root = et.Element('MODBUS_DATA')
                FILE_CREATED = et.SubElement(root, 'FILE_CREATED')
                FILE_CREATED.text = str(calendar.timegm(datetime.datetime.utcnow().timetuple()))

                for row in df.iterrows():
                            
                    meas = et.SubElement(root, 'MEASUREMENT')
                    SOURCE = et.SubElement(meas, 'SOURCE')
                    FIELDKEY = et.SubElement(meas, 'FIELDKEY')
                    VALUE = et.SubElement(meas, 'VALUE')
                    TIMESTAMP = et.SubElement(meas, 'TIMESTAMP')

                    SOURCE.text = str(row[1]['SOURCE'])
                    FIELDKEY.text = str(row[1]['FIELDKEY'])
                    VALUE.text = str(row[1]['VALUE'])
                    TIMESTAMP.text = str(row[1]['TIMESTAMP'])
                    
                with open(path_filename_xml, 'w') as f:
                        f.write('<xml version="1.0"? >')
                        f.write(et.tostring(root).decode('utf-8'))
                print(fileName_xml +' created' )
                os.chdir(os.path.join('/media', 'pi', config_onboard.USB_key))
                os.popen('gzip -c {} > itss_datacollect.zip'.format(fileName_xml))
                return (True, csvFileName, fileName_xml, lastIdentifier, lastTimestamp)
            else:
                Syslogger.log('No new data for... {}'.format(self.registerConfig.cfgAlias()))
                return (False, None, None, None, None)

        except BaseException as ex:
            Syslogger.log('Problem creating xml file... {}'.format(ex))
            return (False, None, None, None, None)
        
        #return (True, csvFileName, filename_xml, lastIdentifier, lastTimestamp)

#    return (False, None, None, None, None)

    def purge(self, maxIdentifier: int, maxTimestamp: int):
        connection, cursor = self._connect()
        if cursor is None:
            Syslogger.log(text="no cursor to DB!", level=syslog.LOG_ERR)
            return (False, None, None, None, None)
        
        connection.row_factory = sqlite3.Row
        tableName = self.registerConfig.cfgAlias()
        sql = "DELETE from {} WHERE identifier <= {} AND ztimestamp <= {}".format(tableName, maxIdentifier, maxTimestamp)
        
        try:
            cursor.execute(sql)
            connection.commit()
            return True
        except:
            return False
        finally:
            self._disconnect(cursor)

    ##helper functions for sqlite

    def _sqlCreateTable(self):
        #header
        result = 'CREATE TABLE IF NOT EXISTS "{}" ('.format(self.registerConfig.cfgAlias())

        #primary key column
        result += '"identifier" Integer NOT NULL PRIMARY KEY AUTOINCREMENT, "ztimestamp" timestamp DEFAULT 0,'

        for row in self.registerConfig.registers().data:
            result += '"{}" {}'.format(row['Name'], row['Type'])
            if row == self.registerConfig.registers().data[-1]:
                continue
            result += ','

        #the end
        result += ');'
        return result

    def _sqlUpdateColumns(self, columns: dict, cursor: sqlite3.Cursor):
        #get the columns from sqlite
        tableName = self.registerConfig.cfgAlias()
        query = "PRAGMA table_info({})".format(tableName)
        cursor.execute(query)
        result = cursor.fetchall()

        #map result against column names
        columnsToAdd = dict(columns)

        #iterate through result and remove from columnsToAdd
        for existingColumn in result:
            #the name seems to be at array pos[1]
            name = existingColumn[1]
            if name in columnsToAdd.keys():
                del columnsToAdd[name]

        if not columnsToAdd:
            return None

        #all column names that don't exist in result are now in columns to add
        for name, type in columnsToAdd.items():
            query = "ALTER TABLE {} ADD '{}' {}".format(tableName, name, type)

        return query

    def _sqlInsertValues(self):
        
        names = []
        values = []

        names.append("ztimestamp")
        values.append(self.registerConfig.registers().timestamp)

        for row in self.registerConfig.registers().data:
            value = row['Value']
            if value is None:
                continue
            names.append(row['Name'])
            values.append(value)

        if (len(names) == 0) or (len(names) != len(values)):
            return None

        #header
        result = 'INSERT INTO "%s" ( ' % (self.registerConfig.cfgAlias())
        result += ', '.join('"%s"' % (name) for name in names)
        result += ') VALUES ('
        result += ', '.join(['?']*len(values))

        # result += ', '.join('{}'.format(value) for value in values)
        result += ')'
        return result, values

    def debugDump(self, filename):
        path = os.path.join('/media', '/pi', config_onboard.USB_key, filename)
        mode = 'a'
        if os.path.exists(path) is False:
            mode = 'w'

        sql, values = self._sqlInsertValues()

        with open(path, mode) as outfile:
            outfile.write(">\n")
            outfile.write(sql)
            outfile.write("\n")
            json.dump(values, outfile)
            outfile.write("\n")
            outfile.write("<\n")
        