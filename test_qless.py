import sys,time,subprocess
import logging
import qless
import utils

logger = logging.getLogger(__name__)

class QlessTester(utils.QueueTester):
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
                qless_cx = qless.client(url='redis://localhost')
                qless_cx.queues.counts
                return
            except Exception as error:
                logger.warn("Error connecting to redis: %s", error)
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
        self.qless_cx = qless.client(url='redis://localhost')
        self.qless_queues_to_watch = queues_to_watch
        self.qless_queues = dict([ (_,self.qless_cx.queues[_]) for _ in queues_to_watch ])

    def send(self, queue, message):
        input_queue = self.qless_cx.queues[queue]
        input_queue.put('test.Stuff', dict(msg=message))

    class Job(object):
        def __init__(self, job):
            self.job = job

        @property
        def queue(self): return self.job.queue_name

        @property
        def body(self): return self.job.data['msg']

        def done(self): self.job.complete()

        # It turns out that calling redis once to create a new job and another to finish the first job
        # is about the same speed as moving a job from Q1 to Q2.
        def move(self, queue): self.job.complete(queue)

    def recv(self, timeout=0):
        for queue_name,queue in self.qless_queues.items():
            job = queue.pop()
            if job:
                return self.Job(job)
        return None

if __name__ == '__main__':
    QlessTester.main()




