import logging
import os
import subprocess
import time
import utils
import nsq
import tornado

logger = logging.getLogger(__name__)

class NsqTester(utils.AsyncQueueTester):
    server_process = None
    server_tmp_dir = 'tmp'

    def start_server(self):
        self.server_process = subprocess.Popen([
            './servers/nsqd',
            '-data-path=%s' % self.server_tmp_dir,
            '-http-address=127.0.0.1:4151',
            '-tcp-address=127.0.0.1:4150'])

        time.sleep(2)

    def stop_server(self):
        if not self.server_process: return
        self.server_process.terminate()
        status = self.server_process.wait()
        if status != 0:
            logger.warn("nsqd exited with %d", status)
        for filename in os.listdir(self.server_tmp_dir):
            if filename.endswith('.dat'):
                path = os.path.join(self.server_tmp_dir,filename)
                logger.info("Removing %s", path)
                os.unlink(path)

    def run_loader(self, msg_generator):
        writer = nsq.Writer(['127.0.0.1:4150'])
        ioloop = tornado.ioloop.IOLoop.instance()
        def send_msg():
            try:
                queue, body = msg_generator.next()
                # print "Sending %s (%s)" % (queue,body)
                writer.pub(queue, queue+':'+body, callback=msg_sent)
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

    class Job(object):
        def __init__(self, job):
            self.job = job
            self.queue,self.body = job.body.split(':',1)
            self.is_done = False

        def done(self):
            self.is_done = True

    def run_worker(self, job_processor, timeout=2):
        ioloop = tornado.ioloop.IOLoop.instance()
        self.writer = nsq.Writer(['127.0.0.1:4150'])
        self.last_recv = time.time()
        def message_handler(job):
            job = self.Job(job)
            job_processor(job)
            self.last_recv = time.time()
            return job.is_done

        readers = { }
        for queue_name in self.queues.keys():
            readers[queue_name] = nsq.Reader(topic=queue_name, channel='test',
                    max_in_flight=100,
                    message_handler=message_handler,
                    nsqd_tcp_addresses=['127.0.0.1:4150'])

        def check_timeout():
            current_time = time.time()
            if current_time - timeout > self.last_recv:
                logger.warn("Worker timeout")
                ioloop.stop()

        tornado.ioloop.PeriodicCallback(check_timeout, 1000).start()
        nsq.run()

    def send(self, queue, message):
        self.writer.pub(queue,queue+':'+message)

if __name__ == '__main__':
    NsqTester.main()


