import logging
import os
import subprocess
import time
import utils
import pika

logger = logging.getLogger(__name__)

class RabbitMqTester(utils.AsyncQueueTester):
    #server_process = None
    #server_tmp_dir = 'tmp'

    #def start_server(self):
    #    self.server_process = subprocess.Popen([
    #        './servers/nsqd',
    #        '-data-path=%s' % self.server_tmp_dir,
    #        '-http-address=127.0.0.1:4151',
    #        '-tcp-address=127.0.0.1:4150'])

    #    time.sleep(2)

    #def stop_server(self):
    #    if not self.server_process: return
    #    self.server_process.terminate()
    #    status = self.server_process.wait()
    #    if status != 0:
    #        logger.warn("nsqd exited with %d", status)
    #    for filename in os.listdir(self.server_tmp_dir):
    #        if filename.endswith('.dat'):
    #            path = os.path.join(self.server_tmp_dir,filename)
    #            logger.info("Removing %s", path)
    #            os.unlink(path)

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        for queue_name in self.queues.keys():
            self.channel.queue_declare(queue=queue_name, durable=True)

    def run_loader(self, msg_generator):
        self.connect()
        for queue, body in msg_generator:
            self.channel.basic_publish(exchange='',
                        routing_key=queue,
                        body=queue+':'+body)
        self.connection.close()

    class Job(object):
        def __init__(self,channel,method,body):
            self.channel = channel
            self.method = method
            self.queue,self.body = body.split(':',1)

        def done(self):
            self.channel.basic_ack(delivery_tag = self.method.delivery_tag)

    def run_worker(self, job_processor, timeout=2):
        self.connect()
        self.last_recv = time.time()
        def check_timeout(signum, frame):
            logger.info("check_timeout")
            current_time = time.time()
            if current_time - timeout > self.last_recv:
                logger.warn("Worker timeout")
                self.connection.close()
                return
            signal.alarm(1)
        import signal
        signal.signal(signal.SIGALRM,check_timeout)
        signal.alarm(1)

        def message_handler(ch,method,properties,body):
            self.last_recv = time.time()
            logger.info("Got message: %s", body)
            job = self.Job(ch,method,body)
            job_processor(job)

        for queue_name in self.queues.keys():
            self.channel.basic_consume(message_handler, queue=queue_name)

        try:
            self.channel.start_consuming()
        except Exception as error:
            logger.error("Connection closed (%s)", error)

    def send(self, queue, message):
        self.writer.pub(queue,queue+':'+message)

if __name__ == '__main__':
    RabbitMqTester.main()




