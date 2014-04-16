import collections
import logging
import os
import sys
import time
import argparse

logger = logging.getLogger(__name__)

def run_processes(functions):
    pids = set()
    try:
        counter = collections.defaultdict(int)
        for function in functions:
            name = function.__name__
            counter[name] += 1
            pid = os.fork()
            if pid == 0:
                # sys.stdout = open("tmp/%s.%d" % (name,counter[name]),"w")
                function()
                sys.exit(0)
            else:
                logger.debug("Spawned process %d", pid)
                pids.add(pid)
        logger.info("Spawned %d processes", len(pids))

        while pids:
            pid, status_code = os.wait()
            if status_code != 0:
                logger.warn("Process %d done (status code: %d)", pid, status_code)
            else:
                logger.debug("Process %d done (status code: %d)", pid, status_code)
            pids.remove(pid)
    except KeyboardInterrupt:
        logger.fatal("Keyboard interrupt")
        for pid in pids:
            os.kill(pid, 15)
        sys.exit(1)


class QueueTester(object):
    def __init__(self, options):
        self.options = options
        self.num_workers = options.num_workers
        self.num_loaders = options.num_loaders
        self.num_jobs = options.num_jobs
        self.num_queues = options.num_queues
        self.msg_size = options.msg_size

        self.queues = {}
        for i in range(0, self.num_queues - 1):
            queue_name = "q%d" % i
            next_queue = "q%d" % (i + 1)
            self.queues[queue_name] = next_queue
        self.queues["q%d" % (self.num_queues - 1)] = ''

    def load(self, num_tasks):
        self.connect(self.queues.keys())
        bytes_sent = 0
        for i in range(num_tasks):
            msg = str(i)

            if self.msg_size > len(msg):
                msg *= self.msg_size/len(msg)
            self.send("q0", msg)
            bytes_sent += len(msg)
        logger.info("Loaded %d tasks, %d bytes", num_tasks, bytes_sent)

    def work(self):
        self.connect(self.queues.keys())

        job_processed = collections.defaultdict(int)
        bytes_processed = 0
        sleep_time = 0
        no_job_loops = 0
        while no_job_loops < 2:
            job = self.recv(timeout=1)
            if job:
                current_queue = job.queue
                next_queue = self.queues.get(current_queue, None)
                if next_queue:
                    if hasattr(job,'move'):
                        job.move(next_queue)
                    else:
                        self.send(next_queue, job.body)
                        job.done()
                else:
                    job.done()
                job_processed[current_queue] += 1
                bytes_processed += len(job.body)
                no_job_loops = 0
            else:
                sleep_time += 0.1
                no_job_loops += 1
        logger.info("Processed %d jobs (%s); slept %.1f seconds; %d bytes processed",
                    sum(job_processed.values()),
                    sorted(job_processed.items()),
                    sleep_time,
                    bytes_processed)

    def start_workers(self):
        def run_loader():
            self.load(self.num_jobs / self.num_loaders)

        processes = [run_loader, ] * self.num_loaders + [self.work, ] * self.num_workers
        start_time = time.time()
        run_processes(processes)
        run_time = time.time() - start_time
        logger.warn("RESULT: Processing %d jobs through %d queues took %f seconds; %.2f jobs/second",
                    self.num_jobs,
                    self.num_queues,
                    run_time,
                    self.num_jobs * self.num_queues / run_time)
        with open("results.txt","a") as result_file:
            result_file.write("%s\t%f\t%f\t%s\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                run_time, self.num_jobs * self.num_queues / run_time,
                " ".join(sys.argv)
            ))


    # Methods to over-ride
    def connect(self, queues):
        raise NotImplementedError()

    def send(self, queue, message):
        raise NotImplementedError()

    def recv(self, timeout=0):
        raise NotImplementedError()

    def start_server(self, queues_to_watch=None):
        logger.warn("No start_server defined")

    def stop_server(self):
        logger.warn("No stop_server defined")
        
    @classmethod
    def add_arguments(cls,argparser):
        argparser.add_argument('num_jobs', type=int)
        argparser.add_argument('num_loaders', type=int)
        argparser.add_argument('num_workers', type=int)
        argparser.add_argument('num_queues', type=int, default=1, nargs='?')
        argparser.add_argument('--msg-size', type=int,
                               help="Size of messages to send around")
        argparser.add_argument('--no-server', action='store_true',
                               help="Don't start queue server")
        argparser.add_argument('-v', '--verbose',
                               action='store_const', const=logging.DEBUG,
                               dest='loglevel', default=logging.INFO,
                               help='Debug output is enabled')
        argparser.add_argument('-q', '--quiet',
                               action='store_const', const=logging.WARN,
                               dest='loglevel', 
                               help='Debug output is enabled')

    @classmethod
    def main(cls):
        ap = argparse.ArgumentParser()
        cls.add_arguments(ap)

        options = ap.parse_args()

        logging.basicConfig(level=options.loglevel,
                            format='%(asctime)s %(name)s[%(process)d] [%(levelname)s] %(message)s'.format(arg0=os.path.basename(sys.argv[0])))

        test_harness = cls(options)

        if not options.no_server:
            test_harness.start_server()

        test_harness.start_workers()

        test_harness.stop_server()


class AsyncQueueTester(QueueTester):
    """Tester for queue that work in Async ways

    The test harness starts loaders and workers as usual but monitors the queue status to see when it's done

    """
    def load(self, num_tasks):
        self.bytes_sent = 0
        def msg_to_send(self,num_tasks):
            for i in range(num_tasks):
                msg = str(i)
                if self.msg_size > len(msg):
                    msg *= self.msg_size/len(msg)
                    self.bytes_sent += len(msg)
                yield ('q0',msg)

        self.run_loader(msg_to_send(self,num_tasks))
        logger.info("Loaded %d tasks, %d bytes", num_tasks, self.bytes_sent)

    def work(self):
        self.job_processed = collections.defaultdict(int)
        self.bytes_processed = 0

        def process_job(job):
            current_queue = job.queue
            next_queue = self.queues.get(current_queue, None)
            if next_queue:
                if hasattr(job,'move'):
                    job.move(next_queue)
                else:
                    self.send(next_queue, job.body)
                    job.done()
            else:
                job.done()
            self.job_processed[current_queue] += 1
            self.bytes_processed += len(job.body)

        self.run_worker(process_job,timeout=2)

        logger.info("Processed %d jobs (%s); %d bytes processed",
                    sum(self.job_processed.values()),
                    sorted(self.job_processed.items()),
                    self.bytes_processed)

    def start_workers(self):
        def run_loader():
            self.load(self.num_jobs / self.num_loaders)

        processes = [run_loader, ] * self.num_loaders + [self.work, ] * self.num_workers
        start_time = time.time()
        run_processes(processes)
        run_time = time.time() - start_time
        logger.warn("RESULT: Processing %d jobs through %d queues took %f seconds; %.2f jobs/second",
                    self.num_jobs,
                    self.num_queues,
                    run_time,
                    self.num_jobs * self.num_queues / run_time)
        with open("results.txt","a") as result_file:
            result_file.write("%s\t%f\t%f\t%s\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                run_time, self.num_jobs * self.num_queues / run_time,
                " ".join(sys.argv)
            ))


    # Methods to over-ride
    def connect(self, queues):
        raise NotImplementedError()

    def run_loader(self, queue, message):
        raise NotImplementedError()

    def recv(self, timeout=0):
        raise NotImplementedError()

    def start_server(self, queues_to_watch=None):
        logger.warn("No start_server defined")

    def stop_server(self):
        logger.warn("No stop_server defined")

