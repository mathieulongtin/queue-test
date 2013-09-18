import argparse,os,sys,time
import logging
import qless

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


class QlessTester(object):
    def __init__(self, num_queues = 1, qless_url='redis://localhost'):
        self.num_queues = num_queues
        self.qless_url = qless_url
        self.qless_cx = qless.client(url=self.qless_url)

    def load(self, num_tasks):
        input_queue = self.qless_cx.queues['q0']
        for i in range(num_tasks):
            input_queue.put('test.Stuff', dict(i=i))
        logger.info("Loaded %d tasks", num_tasks)

    def work(self):
        queues = []
        for i in range(self.num_queues-1):
            queue_name = "q%d" % i
            next_queue = "q%d" % (i+1)
            queues.append((self.qless_cx.queues[queue_name],next_queue))
        queues.append((self.qless_cx.queues["q%d"%(self.num_queues-1)],None))

        job_processed = 0
        sleep_time = 0
        no_job_loops = 0
        while no_job_loops < 2:
            job_found = False
            for queue,next_queue in queues:
                job = queue.pop()
                if job:
                    job_found = True
                    job.complete(next_queue)
                    job_processed += 1
            if not job_found:
                time.sleep(0.1)
                sleep_time += 0.1
                no_job_loops += 1
            else:
                no_job_loops = 0
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
    if len(sys.argv) < 4 or len(sys.argv) > 6:
        print "Usage: test_qless.py #jobs #loaders #workers [#queues [REDISURL]]"
        sys.exit(2)

    num_jobs = int(sys.argv[1])
    num_loaders = int(sys.argv[2])
    num_workers = int(sys.argv[3])
    if len(sys.argv) == 5:
        num_queues = int(sys.argv[4])
    else:
        num_queues = 1
    if len(sys.argv) == 6:
        qless_url = sys.argv[5]
    else:
        qless_url = 'redis://localhost'

    tester = QlessTester(num_queues, qless_url)
    tester.start_workers(num_workers,num_loaders,num_jobs)

if __name__ == '__main__':
    main()


