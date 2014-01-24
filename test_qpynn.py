import sys
import subprocess
import logging
import qpynn
import test_qpy

class QpyNnTester(test_qpy.QpyTester):
    def start_server(self):
        self.server_process = subprocess.Popen([sys.executable,'qpynn.py'])

    def connect(self,queues_to_watch=None):
        self.qpy = qpynn.Client()
        self.qpy_queues_to_watch = queues_to_watch


if __name__ == '__main__':
    QpyNnTester.main()


