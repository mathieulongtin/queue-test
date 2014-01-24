import argparse,os,sys,time,collections,subprocess
import logging
import beanstalkc


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_processes(functions):
    pids = set()
    for function in functions:
        pid = os.fork()
        if pid == 0:
            function()
            sys.exit(0)
        else:
            logger.debug("Spawned process %d", pid)
            pids.add(pid)

    while pids:
        pid,status_code = os.wait()
        logger.debug("Process %d done (status code: %d)", pid, status_code)
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
            self.beanstalk.put("q0:%d" % i)
        logger.info("Loaded %d tasks", num_tasks)

    def work(self):
        self.connect()
        queues = {}
        for i in range(0,self.num_queues-1):
            queue_name = "q%d" % i
            next_queue = "q%d" % (i+1)
            queues[queue_name] = next_queue
        queues["q%d"%(self.num_queues-1)] = ''
        # print "%s" % queues

        job_processed = collections.defaultdict(int)
        sleep_time = 0
        no_job_loops = 0
        while no_job_loops < 2:
            job = self.beanstalk.reserve(timeout=1)
            if job:
                current_queue,i = job.body.split(':')
                next_queue = queues.get(current_queue,None)
                if next_queue:
                    body = next_queue+':'+i
                    self.beanstalk.use(next_queue)
                    self.beanstalk.put(body)
                job.delete()
                job_processed[current_queue] += 1
                no_job_loops = 0
            else:
                sleep_time += 0.1
                no_job_loops += 1
        logger.info("Processed %d jobs (%s); slept %.1f seconds",
                    sum(job_processed.values()),
                    sorted(job_processed.items()),
                    sleep_time)

    def start_workers(self,num_workers,num_loaders,num_jobs):
        def run_loader():
            self.load(num_jobs/num_loaders)
        processes = [ run_loader, ] * num_loaders + [ self.work, ] * num_workers
        start_time = time.time()
        run_processes(processes)
        run_time = time.time()-start_time
        logger.info("Processing %d jobs through %d queues took %f seconds; %.2f jobs/second",
                    num_jobs,
                    self.num_queues,
                    run_time,
                    num_jobs*self.num_queues/run_time)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('num_jobs',type=int)
    ap.add_argument('num_loaders',type=int)
    ap.add_argument('num_workers',type=int)
    ap.add_argument('num_queues',type=int, default=1, nargs='?')
    ap.add_argument('--no-server',action='store_true',
                    help="Don't start beanstalkd server")

    options = ap.parse_args()

    if not options.no_server:
        server_proc = subprocess.Popen(['./servers/beanstalkd'])

    tester = BeanstalkTester(options.num_queues)
    tester.start_workers(options.num_workers,options.num_loaders,options.num_jobs)

    if not options.no_server:
        server_proc.terminate()
        status = server_proc.wait()
        if status != 0:
            logger.warn("beanstalkd exited with %d", status)

if __name__ == '__main__':
    main()


