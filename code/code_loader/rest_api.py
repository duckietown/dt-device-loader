import time
import json
from threading import Thread

PORT = 8081


class CodeLoaderPrinter(Thread):

  def __init__(self, code_loader):
    Thread.__init__(self)
    self.code_loader = code_loader
    self.is_shutdown = False
    self.frequency = 1.0

  def run(self):
    # TODO: re-enable
    # while not self.code_loader.is_shutdown():
    while True:
      code_loader_status = self.code_loader.get_status()
      print(json.dumps(code_loader_status, indent=4, sort_keys=True))
      if self.is_shutdown:
        break
      time.sleep(1.0 / self.frequency)

  def stop(self):
    self.is_shutdown = True
