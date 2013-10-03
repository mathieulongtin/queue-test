import collections
import os
import sys
import time
import logging

import nanomsg
import msgpack
from utils import run_processes


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] %(levelname)s %(message)s')

in_endpoint = 'tcp://127.0.0.1:7893'
out_endpoint = 'tcp://127.0.0.1:7894'
#in_endpoint = 'ipc:///tmp/nn.in'
#out_endpoint = 'ipc:///tmp/nn.out'

class QpyTester(object):
    def __init__(self, num_queues = 1):
        self.num_queues = num_queues
        self.in_socket = None
        self.out_socket = None

    def connect(self, recv=True, send=True):
        if recv:
            self.in_socket = nanomsg.Socket(nanomsg.PULL)
            #self.in_socket.recv_buffer_size = 1
            self.in_socket.connect(in_endpoint)
        if send:
            self.out_socket = nanomsg.Socket(nanomsg.PUSH)
            #self.out_socket.send_buffer_size = 1
            self.out_socket.connect(out_endpoint)

    def send(self,msg,**kwargs):
        print "send\t"+"\t".join(msg)
        return self.out_socket.send(msgpack.dumps(msg),**kwargs)

    def recv(self,**kwargs):
        msg = msgpack.loads(self.in_socket.recv(**kwargs))
        print "recv\t"+"\t".join(msg)
        return msg

    def disconnect(self):
        if self.in_socket:
            self.in_socket.close()
        if self.out_socket:
            self.out_socket.close()

    def load(self, num_tasks):
        self.connect(recv=False)
        self.out_socket.linger = -1
        for i in range(num_tasks):
            self.send(("q0","%d" % i))
        self.disconnect()
        logger.info("Loaded %d tasks", num_tasks)


    def broker(self, num_jobs):
        self.in_socket = nanomsg.Socket(nanomsg.PULL)
        self.in_socket.recv_timeout = 1000
        #self.in_socket.recv_buffer_size = 1
        try:
            self.in_socket.bind(out_endpoint)
        except Exception as error:
            logger.error("Broker bind in: %s", error)
            self.in_socket.connect(out_endpoint)

        self.out_socket = nanomsg.Socket(nanomsg.PUSH)
        #self.out_socket.send_buffer_size = 1
        try:
            self.out_socket.bind(in_endpoint)
        except Exception as error:
            logger.error("Broker bind out: %s", error)
            self.out_socket.connect(in_endpoint)

        start_time = time.time()
        counter = collections.Counter()
        last_sleep = 0
        while counter['total'] < num_jobs:
        #while counter['total'] < num_jobs and counter['sleep'] < 10:
        #while counter['sleep'] < 10:
            msg_parts = None
            try:
                msg_parts = self.recv()
            except nanomsg.NanoMsgAPIError as error:
                if error.errno != nanomsg.EAGAIN:
                    raise
                counter['sleep'] += 1
            if msg_parts:
                self.send(msg_parts)
                counter['total'] += 1
                counter[msg_parts[0]] += 1
            if (counter['total'] % (num_jobs/10) == 0) or (counter['sleep'] > last_sleep):
                logger.info("Mesg copied %s", sorted(counter.items()))
                last_sleep = counter['sleep']
        logger.info("Total mesg copied %s", sorted(counter.items()))
        duration = time.time() - start_time
        logger.info("Duration %.3f; rate %f m/s", duration, counter['total']/duration)
        self.out_socket.close()
        self.in_socket.close()

    def work(self):
        self.connect()
        queues = {}
        for i in range(self.num_queues-1):
            queue_name = "q%d" % i
            next_queue = "q%d" % (i+1)
            queues[queue_name] = next_queue
        queues["q%d"%(self.num_queues-1)] = ''

        job_processed = 0
        sleep_time = 0
        last_status = time.time()
        no_job_loops = 0
        counter = collections.Counter()
        self.in_socket.recv_timeout = 1000
        #while True:
        while no_job_loops < 3:
            try:
                queue,data = self.recv()
                counter[queue] += 1
                next_queue = queues[queue]
                if next_queue:
                    self.send((next_queue,data))
                job_processed += 1
                no_job_loops = 0
            except nanomsg.NanoMsgAPIError as error:
                if error.errno != nanomsg.EAGAIN:
                    raise
                sleep_time += 1
                no_job_loops += 1
            if time.time() - last_status > 10:
                logger.info("Processed %s jobs; slept %.1f seconds", sorted(counter.items()), sleep_time)
                last_status = time.time()
        self.disconnect()
        logger.info("Done; Processed %s jobs; slept %.1f seconds", sorted(counter.items()), sleep_time)

    def start_workers(self,num_workers,num_loaders,num_jobs):
        #broker = threading.Thread(target=self.broker)
        #broker.daemon = True
        #broker.start()
        #time.sleep(1)
        def run_loader():
            self.load(num_jobs/num_loaders)
        def run_broker():
            self.broker(num_jobs*self.num_queues)
        processes = [ run_loader, ] * num_loaders + [ self.work, ] * num_workers + [ run_broker, ]
        start_time = time.time()
        run_processes(processes)
        run_time = time.time()-start_time
        logger.info("Processing %d jobs took %f seconds; %.2f jobs/second", num_jobs, run_time, num_jobs*self.num_queues/run_time)

def main():
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print "Usage: test_qless.py #jobs #loaders #workers [#queues [REDISURL]]"
        sys.exit(2)

    num_jobs = int(sys.argv[1])
    num_loaders = int(sys.argv[2])
    num_workers = int(sys.argv[3])
    if len(sys.argv) == 5:
        num_queues = int(sys.argv[4])
    else:
        num_queues = 1

    tester = QpyTester(num_queues)
    tester.start_workers(num_workers,num_loaders,num_jobs)

if __name__ == '__main__':
    main()


