import collections,logging,os,sys
logger = logging.getLogger(__name__)

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

