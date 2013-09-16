import argparse,os,sys
import logging
import qless

logger = logging.getLogger(__name__)

def run_processes(function,num_processes):
    for i in range(num_processes):
        pid = os.fork()
        if pid == 0:
            function()
        else:
            logger.info("Spawned process %d", pid)

    while num_processes > 0:
        pid,status_code = os.wait()
        logger.info("Process %d done (status code: %d)", pid, status_code)


class QlessTester(object):
    def __init__(self, num_queues = 1):
        self.num_queues = num_queues
        self.qless_url = 'redis://localhost'
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
            next_queue = "q%d" % i+1
            queues.append((self.qless_cx.queues[queue_name],next_queue))
        queues.append((self.qless_cs.queues["q%d"%(self.num_queues-1]),None))

        for queue,next_queue in queues:
            job = self.qless_cx.queues[queue_name]
            if job:
                job.complete(next_queue)

    def start_worker(self,num_workers):
        run_processes(self.work, num_workers)

    def start_loader(self,num_loaders,num_tasks):
        def run():
            self.load(num_tasks/num_loaders)
        run_processes(run, num_loaders)

    def start_redis(self):
        
