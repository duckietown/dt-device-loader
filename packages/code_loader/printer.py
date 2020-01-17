import time
import json
from threading import Thread


class CodeLoaderPrinter(Thread):

    def __init__(self, code_loader):
        Thread.__init__(self)
        self._code_loader = code_loader
        self._is_shutdown = False
        self._frequency = 1.0

    def run(self):
        while not self._code_loader.is_shutdown():
            code_loader_status = self._code_loader.get_status()
            print(json.dumps(code_loader_status, indent=4, sort_keys=True))
            if self.is_shutdown():
                break
            time.sleep(1.0 / self._frequency)

    def is_shutdown(self):
        return self._is_shutdown

    def stop(self):
        self._is_shutdown = True
