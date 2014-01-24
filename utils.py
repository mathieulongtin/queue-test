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
        counter = collections.Counter()
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
    def __init__(self, num_workers, num_loaders, num_jobs, num_queues):
        self.num_workers = num_workers
        self.num_loaders = num_loaders
        self.num_jobs = num_jobs
        self.num_queues = num_queues

        self.queues = {}
        for i in range(0, self.num_queues - 1):
            queue_name = "q%d" % i
            next_queue = "q%d" % (i + 1)
            self.queues[queue_name] = next_queue
        self.queues["q%d" % (self.num_queues - 1)] = ''

    def load(self, num_tasks):
        self.connect(self.queues.keys())
        for i in range(num_tasks):
            self.send("q0", str(i))
        logger.info("Loaded %d tasks", num_tasks)

    def work(self):
        self.connect(self.queues.keys())

        job_processed = collections.defaultdict(int)
        sleep_time = 0
        no_job_loops = 0
        while no_job_loops < 2:
            job = self.recv(timeout=1)
            if job:
                current_queue = job.queue
                next_queue = self.queues.get(current_queue, None)
                if next_queue:
                    self.send(next_queue, job.body)
                job.done()
                job_processed[current_queue] += 1
                no_job_loops = 0
            else:
                sleep_time += 0.1
                no_job_loops += 1
        logger.info("Processed %d jobs (%s); slept %.1f seconds",
                    sum(job_processed.values()),
                    sorted(job_processed.items()),
                    sleep_time)

    def start_workers(self):
        def run_loader():
            self.load(self.num_jobs / self.num_loaders)

        processes = [run_loader, ] * self.num_loaders + [self.work, ] * self.num_workers
        start_time = time.time()
        run_processes(processes)
        run_time = time.time() - start_time
        logger.info("Processing %d jobs through %d queues took %f seconds; %.2f jobs/second",
                    self.num_jobs,
                    self.num_queues,
                    run_time,
                    self.num_jobs * self.num_queues / run_time)

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
    def main(cls):
        ap = argparse.ArgumentParser()
        ap.add_argument('num_jobs', type=int)
        ap.add_argument('num_loaders', type=int)
        ap.add_argument('num_workers', type=int)
        ap.add_argument('num_queues', type=int, default=1, nargs='?')
        ap.add_argument('--no-server', action='store_true',
                        help="Don't start beanstalkd server")
        ap.add_argument('-v', '--verbose', action='store_true',
                        help='Debug output is enabled')

        options = ap.parse_args()

        logging.basicConfig(level=logging.DEBUG if options.verbose else logging.INFO)

        test_harness = cls(options.num_workers,
                           options.num_loaders,
                           options.num_jobs,
                           options.num_queues)

        if not options.no_server:
            test_harness.start_server()

        test_harness.start_workers()

        test_harness.stop_server()

