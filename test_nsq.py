import logging
import subprocess
import time
import utils
import nsq
import tornado

logger = logging.getLogger(__name__)

class NsqTester(utils.AsyncQueueTester):
    server_process = None

    def start_server(self):
        self.server_process = subprocess.Popen([
            './bin/nsqd',
            '-http-address="127.0.0.1:4151"',
            '-tcp-address="127.0.0.1:4150"'])

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != -15:
            logger.warn("beanstalkd exited with %d", status)

    def run_loader(self, msg_generator):
        writer = nsq.Writer(['127.0.0.1:4150'])
        ioloop = tornado.ioloop.IOLoop.instance()
        def send_msg():
            try:
                queue, body = msg_generator.next()
                # print "Sending %s (%s)" % (queue,body)
                writer.pub(queue,body, callback=msg_sent)
            except StopIteration:
                ioloop.stop()
        def msg_sent(conn,data):
            if data != 'OK':
                logger.error("Error sending message: %s" % data)
                import sys
                sys.exit(1)
            send_msg()

        ioloop.add_timeout(time.time()+1,send_msg)
        # tornado.ioloop.PeriodicCallback(send_msg, 1).start()
        #writer.on('connect',send_msg)
        nsq.run()

    def connect(self,queues_to_watch=None):
        self.nsq_writer = nsq.Writer(['127.0.0.1:4150'])
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
    NsqTester.main()


