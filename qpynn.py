"""qpy - Naive queueing system

Based roughly on beanstalkd protocol.

This was done strictly to compare to other queues. It is in the same speed class as
beanstalkd, although it offers significantly less features. Part of the speed gains
seems to be from 0mq using a separate CPU thread for handling I/O.

- Uses nanomsg for communication

TODO
- Invalidate a reservation (can't cancel a job if held by another worker)
- Command line argument to set endpoint and other things

"""
import collections,logging,time,uuid
import nanomsg
import msgpack

default_endpoint = 'tcp://127.0.0.1:7890'
logger = logging.getLogger(__name__)

class Job(object):
    __slots__ = ('jid','body','queue','client')
    def __init__(self,jid,body,queue,client):
        self.jid = jid
        self.body = body
        self.queue = queue
        self.client = client

    def cancel(self):
        return self.client.cancel(self.jid)
    delete = cancel

class QpyError(Exception):
    pass


class Client(object):
    def __init__(self,endpoint=default_endpoint):
        self.endpoint = endpoint
        self._socket = nanomsg.Socket(nanomsg.REQ)
        self._socket.connect(self.endpoint)

    def _send_command(self,command,*args):
        message = msgpack.dumps((command,)+args)
        self._socket.send(message)
        results = msgpack.loads(self._socket.recv())
        if results[0] == 'ERR':
            raise QpyError(results[1])
        return [ _ if _ != '' else None for _ in results ]

    def put(self,queue,body,ttr=None):
        jid, = self._send_command('put',queue,body,ttr)
        return jid

    def reserve(self,queues):
        if not isinstance(queues,list):
            queues = list((queues,))
        result = self._send_command('reserve',*queues)
        if result[0] is None:
            return None
        job = Job(result[0],result[1],result[2],self)
        return job

    def cancel(self,jid):
        return self._send_command('cancel',jid)

    def stats(self):
        return self._send_command('stats')

class Server(object):
    default_ttr = 60

    class JobItem(object):
        """Server side job item"""
        __slots__ = ('jid','body','ttr','queue')

        def __init__(self,body,jid=None,ttr=None):
            self.body = body
            self.ttr = ttr

    def __init__(self,endpoint=default_endpoint):
        self.endpoint = endpoint

        self.last_jid = 0
        self.queues = collections.defaultdict(collections.deque) # queue_name -> [ jid, jid, jid ]
        self.job_state = { } # jid -> state # if other than waiting
        self.job_details = { } # jid -> JobItem
        self.job_queues = { } # jid -> body

    def get_new_jid(self):
        self.last_jid += 1
        return str(self.last_jid)

    def put_cmd(self,queue_name,body,ttr=None):
        jid = self.get_new_jid()
        if ttr == '':
            ttr = None
        job = self.JobItem(body,ttr)
        self.job_details[jid] = job
        self.job_queues[jid] = queue_name
        self.queues[queue_name].append(jid)
        return (jid,)

    def reserve_cmd(self, *queues):
        for queue_name in queues:
            if queue_name not in self.queues:
                continue
            queue = self.queues[queue_name]
            try:
                jid = queue.popleft()
                job = self.job_details[jid]
                self.job_state[jid] = ['reserved', time.time() + (job.ttr or self.default_ttr)]
                return (str(jid),job.body,queue_name)
            except IndexError:
                pass
        return ('',)

    def cancel_cmd(self,jid):
        try:
            queue_name = self.job_queues[jid]
            if jid in self.job_state:
                del self.job_state[jid]
            else:
                self.queues[queue_name].remove(jid)
            del self.job_details[jid]
            del self.job_queues[jid]
            return ("1",)
        except KeyError:
            return ("ERR", "Cancel failed: JID not found")

    def stats_cmd(self):
        stats = { }
        for queue_name,queue in self.queues.items():
            stats['%s:waiting' % queue_name] = len(queue)
        stats['reserved'] = len(self.job_state)
        stats['job_count'] = len(self.job_details)
        return ( repr(stats), )

    def check_ttr(self):
        logger.debug("check_ttr")
        now = time.time()
        for jid,state in self.job_state.items():
            if state[1] < now:
                del self.job_state[jid]
                self.queues[self.job_queues[jid]].append(jid)

    def run_server(self):
        logging.basicConfig(level=logging.WARN,format='%(asctime)s [%(process)s] %(levelname)s %(message)s')

        socket = nanomsg.Socket(nanomsg.REP)
        socket.bind(self.endpoint)
        socket.recv_timeout = 1000

        last_check_ttr = time.time()
        while True:
            if time.time() - last_check_ttr > 1:
                self.check_ttr()

            try:
                msg = msgpack.loads(socket.recv())
                command = msg.pop(0)+'_cmd'
                msg = [ (_ if _ != '' else None) for _ in msg ]
                logger.debug("Command %s %s", command, msg)
                if hasattr(self,command):
                    result = getattr(self,command)(*msg)
                    socket.send(msgpack.dumps(result))
                else:
                    socket.send_multipart(msgpack.dumps('ERR','Unknown command "%s"' % command[0:-4]))
            except nanomsg.NanoMsgAPIError as error:
                if error.errno != nanomsg.EAGAIN:
                    raise

if __name__ == '__main__':
    Server().run_server()






