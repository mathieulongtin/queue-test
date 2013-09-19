import argparse,os,sys,time
import logging
import beanstalkc

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def run_processes(functions):
    pids = set()
    for function in functions:
        pid = os.fork()
        if pid == 0:
            function()
            sys.exit(0)
        else:
            logger.info("Spawned process %d", pid)
            pids.add(pid)

    while pids:
        pid,status_code = os.wait()
        logger.info("Process %d done (status code: %d)", pid, status_code)
        pids.remove(pid)

class BeanstalkTester(object):
    def __init__(self, num_queues = 1):
        self.num_queues = num_queues

    def connect(self):
        self.beanstalk = beanstalkc.Connection()
        for i in range(self.num_queues):
            self.beanstalk.watch('q%d' % i)

    def load(self, num_tasks):
        self.connect()
        self.beanstalk.use('q0')
        for i in range(num_tasks):
            self.beanstalk.put("q1:%d" % i)
        logger.info("Loaded %d tasks", num_tasks)

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
        while no_job_loops < 2:
            job = self.beanstalk.reserve(timeout=1)
            if job:
                next_queue,i = job.body.split(':')
                if next_queue:
                    body = queues[next_queue]+':'+i
                    self.beanstalk.use(next_queue)
                    self.beanstalk.put(body)
                job.delete()
                job_processed += 1
                no_job_loops = 0
            else:
                sleep_time += 0.1
                no_job_loops += 1
        logger.info("Processed %d jobs; slept %.1f seconds", job_processed, sleep_time)

    def start_workers(self,num_workers,num_loaders,num_jobs):
        def run_loader():
            self.load(num_jobs/num_loaders)
        processes = [ run_loader, ] * num_loaders + [ self.work, ] * num_workers
        start_time = time.time()
        run_processes(processes)
        run_time = time.time()-start_time
        logger.info("Processing %d jobs took %f seconds; %.2f jobs/second", num_jobs, run_time, num_jobs/run_time)

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

    tester = BeanstalkTester(num_queues)
    tester.start_workers(num_workers,num_loaders,num_jobs)

if __name__ == '__main__':
    main()


