import argparse,collections,os,sys,threading,time
import logging
import zmq
from utils import run_processes

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] %(levelname)s %(message)s')

in_endpoint = 'tcp://127.0.0.1:7893'
out_endpoint = 'tcp://127.0.0.1:7894'

class QpyTester(object):
    in_socket = None
    out_socket = None
    def __init__(self, num_queues = 1):
        self.num_queues = num_queues

    def connect(self, recv=True,send=True):
        self.zmq_context = zmq.Context()
        if recv:
            self.in_socket = self.zmq_context.socket(zmq.PULL)
            self.in_socket.connect(in_endpoint)
            #self.in_socket.setsockopt(zmq.SNDHWM,1)
            #self.in_socket.setsockopt(zmq.RCVHWM,1)
        if send:
            self.out_socket = self.zmq_context.socket(zmq.PUSH)
            self.out_socket.connect(out_endpoint)
            #self.out_socket.setsockopt(zmq.SNDHWM,1)
            #self.out_socket.setsockopt(zmq.RCVHWM,1)

    def disconnect(self):
        if self.in_socket:
            self.in_socket.close(1000)
        if self.out_socket:
            self.out_socket.close(1000)
        #time.sleep(10)

    def load(self, num_tasks):
        self.connect(recv=False)
        for i in range(num_tasks):
            self.out_socket.send_multipart(["q0","%d" % i])
        self.disconnect()
        logger.info("Loaded %d tasks", num_tasks)


    def broker(self, num_jobs):
        self.zmq_context = zmq.Context()
        self.in_socket = self.zmq_context.socket(zmq.PULL)
        self.in_socket.bind(out_endpoint)
        #self.in_socket.hwm = 1
        self.out_socket = self.zmq_context.socket(zmq.PUSH)
        self.out_socket.bind(in_endpoint)
        #self.out_socket.hwm = 1

        start_time = time.time()
        counter = collections.defaultdict(int)
        last_sleep = 0
        while counter['total'] < num_jobs:
            if not self.in_socket.poll(timeout=100):
                counter['sleep'] += 1
            else:
                msg_parts = self.in_socket.recv_multipart()
                self.out_socket.send_multipart(msg_parts)
                counter['total'] += 1
                counter[msg_parts[0]] += 1
            if (counter['total'] % (num_jobs/10) == 0) or (counter['sleep'] > last_sleep):
                logger.info("Mesg copied %s", counter)
                last_sleep = counter['sleep']
        logger.info("Total mesg copied %s", counter)
        duration = time.time() - start_time
        logger.info("Duration %.3f; rate %f m/s", duration, counter['total']/duration)
        self.zmq_context.term()

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
        no_job_loops = 0
        counter = collections.defaultdict(int)
        self.in_socket.poll(timeout=1000)
        while no_job_loops < 10:
            try:
                queue,data = self.in_socket.recv_multipart(zmq.NOBLOCK)
                counter[queue] += 1
                next_queue = queues[queue]
                if next_queue:
                    self.out_socket.send_multipart([next_queue,data])
                job_processed += 1
                no_job_loops = 0
            except zmq.error.Again:
                time.sleep(0.1)
                sleep_time += 0.1
                no_job_loops += 1
        self.disconnect()
        logger.info("Processed %s jobs; slept %.1f seconds", sorted(counter.items()), sleep_time)

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


