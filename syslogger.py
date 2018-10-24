import syslog
import sys

class Syslogger(object):

    @staticmethod
    def log(text, level=syslog.LOG_DEBUG):
        if sys.platform == 'linux':
            print('{}\n'.format(text))
        syslog.syslog(level, text)