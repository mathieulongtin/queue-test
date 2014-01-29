import logging
import os
import subprocess
import time
import utils
import kombu, kombu.pools, kombu.common

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

    @classmethod
    def add_arguments(cls,argparser):
        super(RabbitMqTester,cls).add_arguments(argparser)
        argparser.add_argument('--not-durable',
                dest='durable', action='store_false', default=True,
                help="Make queues not durable")


    def connect(self):
        self.connection = kombu.Connection('amqp://')
        
        if self.options.durable:
            prefix = 'dur_'
        else:
            prefix = 'tmp_'
        self.task_exchange = kombu.Exchange(prefix+'test', type='direct', durable=self.options.durable)
        self.producer = kombu.Producer(self.connection, exchange=self.task_exchange)
        kombu.common.maybe_declare(self.task_exchange, self.connection)
        self.task_queues = [ ]
        for queue_name in self.queues.keys():
            queue = kombu.Queue(prefix+queue_name,
                    self.task_exchange, routing_key=queue_name,
                    durable=self.options.durable)
            self.task_queues.append(queue)
            kombu.common.maybe_declare(queue, self.connection)

    def run_loader(self, msg_generator):
        self.connect()
        for queue, body in msg_generator:
            self.send(queue,body)

        start = time.time()
        self.connection.release()
        release_time = time.time() - start
        if release_time > 1.0:
            logger.info("Connection release time: %f s", release_time)

    class Job(object):
        def __init__(self,message,body):
            self.message = message
            self.queue,self.body = body

        def done(self):
            self.message.ack()

    def run_worker(self, job_processor, timeout=2):
        self.connect()

        def message_handler(body, message):
            self.last_recv = time.time()
            #logger.info("Got message: %s", body)
            job = self.Job(message,body)
            job_processor(job)

        with kombu.Consumer(self.connection, queues=self.task_queues, callbacks=[message_handler]) as consumer:
            while True:
                try:
                    self.connection.drain_events(timeout=1)
                except Exception as error:
                    logger.error("Drain error: %s", error)
                    break

    def send(self, queue, message):
        self.producer.publish([queue,message], 
                routing_key=queue,
                serializer='json',
                delivery_mode=2 if self.options.durable else 1)

if __name__ == '__main__':
    RabbitMqTester.main()




