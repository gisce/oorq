# -*- coding: utf-8 -*-
from hashlib import sha1

from rq import Queue
from rq import get_current_job
from oorq import setup_redis_connection
from oorq.exceptions import *

from tasks import make_chunks, execute
from tools import config
import netsvc


def log(msg, level=netsvc.LOG_INFO):
    logger = netsvc.Logger()
    logger.notifyChannel('oorq', level, msg)


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
            current_job = get_current_job()
            if not args[-1] == token and not current_job:
                # Add the token as a last argument
                args += (token,)
                # Default arguments
                osv_object = args[0]._name
                dbname = args[1].dbname
                uid = args[2]
                fname = f.__name__
                redis_conn = setup_redis_connection()
                q = Queue(self.queue, default_timeout=self.timeout,
                          connection=redis_conn, async=self.async)
                # Pass OpenERP server config to the worker
                conf_attrs = dict(
                    [(attr, value) for attr, value in config.options.items()]
                )
                job = q.enqueue(execute, conf_attrs, dbname, uid, osv_object,
                                fname, *args[3:])
                log('Enqueued job (id:%s): [%s] pool(%s).%s%s'
                        % (job.id, dbname, osv_object, fname, args[2:]))
                return job.result
            else:
                # Remove the token
                if args[-1] == token:
                    args = args[:-1]
                return f(*args, **kwargs)
        return f_job


class split_job(job):
    """This will split default OpenObject function ids parameters.
    """
    def __init__(self, *args, **kwargs):
        super(split_job, self).__init__(*args, **kwargs)
        self.n_chunks = 4

    def __call__(self, f):
        token = sha1(f.__name__).hexdigest()

        def f_job(*args, **kwargs):
            current_job = get_current_job()
            if not args[-1] == token and not current_job:
                # Add the token as a last argument
                args += (token,)
                # Default arguments
                osv_object = args[0]._name
                dbname = args[1].dbname
                uid = args[2]
                ids = args[3]
                if not isinstance(ids, (list, tuple)):
                    raise OORQNotIds()
                
                fname = f.__name__
                redis_conn = setup_redis_connection()
                q = Queue(self.queue, default_timeout=self.timeout,
                          connection=redis_conn, async=self.async)
                # Pass OpenERP server configuration to the worker
                conf_attrs = dict(
                    [(attr, value) for attr, value in config.options.items()]
                )
                jobs = []
                for idx, chunk in enumerate(make_chunks(ids,self.n_chunks)):
                    args[3] = chunk
                    job = q.enqueue(execute, conf_attrs, dbname, uid,
                                    osv_object, fname, *args[3:])
                    log('Enqueued split (%s/%s) job (id:%s): [%s] pool(%s).%s%s'
                            % (idx, self.n_chunksjob.id, dbname, osv_object,
                               fname, args[2:]))
                    jobs.append(job.id)
                return jobs
            else:
                # Remove the token
                if args[-1] == token:
                    args = args[:-1]
                return f(*args, **kwargs)
        return f_job
