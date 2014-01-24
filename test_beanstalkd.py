import subprocess
import beanstalkc
import utils

class BeanstalkdTester(utils.QueueTester):
    server_process = None

    def start_server(self):
        self.server_process = subprocess.Popen(['./servers/beanstalkd'])

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != -15:
            logger.warn("beanstalkd exited with %d", status)

    def connect(self,queues_to_watch=None):
        self.beanstalk = beanstalkc.Connection()
        self.last_queue = None
        if queues_to_watch:
            for q in queues_to_watch:
                self.beanstalk.watch(q)

    def send(self, queue, message):
        if self.last_queue != queue:
            self.beanstalk.use(queue)
            self.last_queue = queue
        self.beanstalk.put(queue+":"+message)

    class Job(object):
        def __init__(self, job):
            self.job = job
            self.queue,self.body = job.body.split(':',1)

        def done(self):
            self.job.delete()

    def recv(self, timeout=0):
        job = self.beanstalk.reserve(timeout=timeout)
        if not job:
            return None
        return self.Job(job)

if __name__ == '__main__':
    BeanstalkdTester.main()


