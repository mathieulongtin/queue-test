import argparse,os,sys,time,subprocess
import logging
import qpy
import utils


class QpyTester(utils.QueueTester):
    server_process = None

    def start_server(self):
        self.server_process = subprocess.Popen([sys.executable,'qpy.py'])

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != -15:
            logger.warn("qpy exited with %d", status)

    def connect(self,queues_to_watch=None):
        self.qpy = qpy.Client()
        self.qpy_queues_to_watch = queues_to_watch

    def send(self, queue, message):
        self.qpy.put(queue,message)

    class Job(object):
        def __init__(self, job):
            self.job = job

        @property
        def queue(self): return self.job.queue

        @property
        def body(self): return self.job.body

        def done(self): self.job.delete()

    def recv(self, timeout=0):
        job = self.qpy.reserve(self.qpy_queues_to_watch)
        if not job:
            return None
        return self.Job(job)

if __name__ == '__main__':
    QpyTester.main()

