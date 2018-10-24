from threading import Timer

class RepeatingTimer(Timer):
    """See: https://github.com/python-git/python/blob/master/Lib/threading.py"""

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)
            
        self.finished.set()