import argparse,os,sys,time,subprocess
import logging
import qredis
import utils
import redis

logger = logging.getLogger(__name__)

class QpyTester(utils.QueueTester):
    server_process = None

    def start_server(self):
        self.server_process = subprocess.Popen([
            './servers/redis-server',
            '--save','',
            '--loglevel','warning',
        ])
        timeout = time.time() + 10
        while time.time() < timeout:
            try:
                client = qredis.Client()
                client.stats()
                return
            except Exception as error:
                # logger.warn("Error connecting to redis: %s", error)
                time.sleep(1)
        self.server_process.kill()
        raise OSError("Redis not answering after 10 seconds")

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != -15:
            logger.warn("redis exited with %d", status)
            
    def connect(self,queues_to_watch=None):
        self.client = qredis.Client()
        self.queues_to_watch = queues_to_watch

    def send(self, queue, message):
        self.client.put(queue,message)

    def recv(self, timeout=0):
        return self.client.reserve(self.queues_to_watch)

if __name__ == '__main__':
    QpyTester.main()

