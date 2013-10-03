import collections
import os
import sys
import time
import logging

import nanomsg
import msgpack


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] %(levelname)s %(message)s')

in_endpoint = 'tcp://127.0.0.1:7893'
out_endpoint = 'tcp://127.0.0.1:7894'

def run_processes(functions):
    try:
        pids = set()
        counter = collections.Counter()
        for function in functions:
            name = function.__name__
            counter[name] += 1
            pid = os.fork()
            if pid == 0:
                sys.stdout = open("tmp/%s.%d" % (name,counter[name]),"w")
                function()
                sys.exit(0)
            else:
                #logger.info("Spawned process %d", pid)
                pids.add(pid)
        logger.info("Spawned %d processes", len(pids))

        while pids:
            pid,status_code = os.wait()
            if status_code != 0:
                logger.info("Process %d done (status code: %d)", pid, status_code)
            pids.remove(pid)
    except KeyboardInterrupt:
        logger.fatal("Keyboard interrupt")
        sys.exit(1)

class QpyTester(object):
    def __init__(self, num_queues = 1):
        self.num_queues = num_queues

    def connect(self):
        self.in_socket = nanomsg.Socket(nanomsg.PULL)
        self.in_socket.connect(in_endpoint)
        self.in_socket.recv_buffer_size = 1
        self.out_socket = nanomsg.Socket(nanomsg.PUSH)
        self.out_socket.connect(out_endpoint)
        self.in_socket.send_buffer_size = 1

    def disconnect(self):
        self.in_socket.close()
        self.out_socket.close()

    def load(self, num_tasks):
        self.connect()
        for i in range(num_tasks):
            self.send(("q0","%d" % i))
        self.disconnect()
        logger.info("Loaded %d tasks", num_tasks)

    def send(self,msg,**kwargs):
        print "send\t"+"\t".join(msg)
        return self.out_socket.send(msgpack.dumps(msg),**kwargs)
    def recv(self,**kwargs):
        msg = msgpack.loads(self.in_socket.recv(**kwargs))
        print "recv\t"+"\t".join(msg)
        return msg

    def broker(self, num_jobs):
        self.in_socket = nanomsg.Socket(nanomsg.PULL)
        self.in_socket.bind(out_endpoint)
        self.in_socket.recv_timeout = 1000
        self.in_socket.recv_buffer_size = 1
        self.out_socket = nanomsg.Socket(nanomsg.PUSH)
        self.out_socket.bind(in_endpoint)
        self.out_socket.send_buffer_size = 1

        start_time = time.time()
        counter = collections.Counter()
        last_sleep = 0
        #while counter['total'] < num_jobs and counter['sleep'] < 10:
        while counter['sleep'] < 10:
            try:
                msg_parts = self.recv()
                self.send(msg_parts)
                counter['total'] += 1
                counter[msg_parts[0]] += 1
            except nanomsg.NanoMsgAPIError as error:
                if error.errno != nanomsg.EAGAIN:
                    raise
                counter['sleep'] += 1
            if (counter['total'] % (num_jobs/10) == 0) or (counter['sleep'] > last_sleep):
                logger.info("Mesg copied %s", counter)
                last_sleep = counter['sleep']
        logger.info("Total mesg copied %s", counter)
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
        no_job_loops = 0
        counter = collections.Counter()
        self.in_socket.recv_timeout = 1000
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
        self.disconnect()
        logger.info("Processed %s jobs; slept %.1f seconds", sorted(counter.items()), sleep_time)

    def start_workers(self,num_workers,num_loaders,num_jobs):
        #broker = threading.Thread(target=self.broker)
        #broker.daemon = True
        #broker.start()
        #time.sleep(1)
        def run_loader():
            time.sleep(1)
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


