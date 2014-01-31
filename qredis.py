"""qredis - Naive redis system

Based roughly on beanstalkd protocol.

This was done strictly to compare to other queues. It is in the same speed class as
beanstalkd, although it offers significantly less features. Part of the speed gains
seems to be from 0mq using a separate CPU thread for handling I/O. It would be interesting
to modify beanstalkd to use 0mq as transport instead of TCP.

- Uses 0mq for communication

TODO
- Invalidate a reservation (can't cancel a job if held by another worker)
- Command line option to set endpoint
- Backup to disk (periodic or incremental)

"""
import collections,logging,time,uuid
import json
import redis

default_endpoint = 'redis://localhost'
logger = logging.getLogger(__name__)

class Job(object):
    __slots__ = ('body','queue','client')
    def __init__(self,queue,body,client):
        self.body = body
        self.queue = queue
        self.client = client

    def requeue(self):
        return self.client.put(self.queue,self.body)
    def done(self):
        pass

class QpyError(Exception):
    pass


class Client(object):
    def __init__(self,endpoint=default_endpoint):
        self.redis = redis.StrictRedis.from_url(url=endpoint)

    def put(self,queue,body,ttr=None):
        self.redis.rpush('q:'+queue, body)

    def reserve(self,queues):
        for queue in queues:
            body = self.redis.lpop('q:'+queue)
            if body:
                return Job(queue,body,self)
        return None

    def stats(self):
        stats = {}
        for key in self.redis.keys('q:*'):
            queue = key[2:]
            stats[queue] = self.redis.llen(key)
        return stats








