import argparse,os,sys,time,subprocess
import logging
import utils
import sys
sys.path.insert(0, './lib/pydisque')
import pydisque.client

logger = logging.getLogger(__name__)

class DisqueTester(utils.QueueTester):
    server_process = None

    def start_server(self):
        stderr = open('/dev/null', 'w') if self.options.loglevel == logging.WARN else None
        self.server_process = subprocess.Popen(
            ['./servers/disque/src/disque-server', '--loglevel', 'warning'],
            stdout=stderr,
            stderr=stderr
        )
        timeout = time.time() + 10
        while time.time() < timeout:
            try:
                client = pydisque.client.Client()
                client.connect()
                client.info()
                return
            except Exception as error:
                # logger.warn("Error connecting to disque: %s", error)
                time.sleep(1)
        self.server_process.kill()
        raise OSError("Disque not answering after 10 seconds")

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != 0:
            logger.warn("disque exited with %d", status)
            
    def connect(self, queues_to_watch=None):
        self.client = pydisque.client.Client()
        self.client.connect()
        self.queues_to_watch = queues_to_watch

    def send(self, queue, message):
        self.client.add_job(queue, message)

    class Job(object):
        def __init__(self, queue, id, payload, client):
            self.queue = queue
            self.body = payload
            self.id = id
            self.client = client

        def done(self):
            return self.client.ack_job(self.id)

        def requeue(self):
            return self.client.nack(self.id)

    def recv(self, timeout=0):
        jobs = self.client.get_job(self.queues_to_watch, timeout=10)
        if not jobs:
            return None
        queue, id_, payload = jobs[0]
        job = self.Job(queue, id_, payload, self.client)
        return job

if __name__ == '__main__':
    DisqueTester.main()

