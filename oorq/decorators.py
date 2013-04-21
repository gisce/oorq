# -*- coding: utf-8 -*-
import time
from hashlib import sha1

from rq import Connection, Queue
from redis import Redis

from tasks import execute
from tools import config

class job(object):
    def __init__(self, *args, **kwargs):
        self.async = True
        self.queue = 'default'
        self.timeout = None
        # Assign all the arguments to attributes
        for arg, value in kwargs.items():
            setattr(self, arg, value)

    def __call__(self, f):
        token = sha1(f.__name__).hexdigest()
        def f_job(*args, **kwargs):
            if not args[-1] == token:
                # Add the token as a last argument
                args += (token,)
                # Default arguments
                osv_object = args[0]._name
                dbname = args[1].dbname
                uid = args[2]
                fname = f.__name__
                redis_conn = Redis()
                q = Queue(self.queue, default_timeout=self.timeout,
                          connection=redis_conn, async=self.async)
                job = q.enqueue(execute, config['addons_path'], dbname, uid,
                                osv_object, fname, *args[3:])
                return job.result
            else:
                # Remove the token
                args = args[:-1]
                return f(*args, **kwargs)
        return f_job
