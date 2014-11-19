# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
from multiprocessing import cpu_count
import logging

from rq import Queue
from rq import get_current_job
from .oorq import setup_redis_connection, set_hash_job
from .utils import config_from_environment
from .exceptions import *

from .tasks import make_chunks, execute, isolated_execute
from openerp.tools import config
from openerp import netsvc


def log(msg, level=logging.INFO):
    logger = logging.getLogger('oorq')
    logger.log(level, msg)


class job(object):
    def __init__(self, *args, **kwargs):
        self.async = True
        self.queue = 'default'
        self.timeout = None
        # Assign all the arguments to attributes
        config = config_from_environment('OORQ', **kwargs)
        for arg, value in config.items():
            setattr(self, arg, value)

    def __call__(self, f):
        token = sha1(f.__name__).hexdigest()

        def f_job(*args, **kwargs):
            current_job = get_current_job()
            if not args[-1] == token and not current_job and self.async:
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
                                fname, *args[3:], **kwargs)
                hash = set_hash_job(job)
                log('Enqueued job (id:%s): [%s] pool(%s).%s%s'
                    % (job.id, dbname, osv_object, fname, args[2:]))
                return job
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
        self.isolated = False
        # Default make chunks as processors we have + 1
        self.n_chunks = cpu_count() + 1
        self.chunk_size = None
        super(split_job, self).__init__(*args, **kwargs)
        # If size of chunks is assigned don't use n_chunks
        if self.chunk_size:
            self.n_chunks = None

    def __call__(self, f):
        token = sha1(f.__name__).hexdigest()

        def f_job(*args, **kwargs):
            current_job = get_current_job()
            if not args[-1] == token and not current_job and self.async:
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
                if self.isolated:
                    task = isolated_execute
                    mode = 'isolated'
                else:
                    mode = 'not isolated'
                    task = execute
                # We have to convert args to list
                args = list(args)
                chunks = make_chunks(ids, n_chunks=self.n_chunks,
                                     size=self.chunk_size)
                for idx, chunk in enumerate(chunks):
                    args[3] = chunk
                    job = q.enqueue(task, conf_attrs, dbname, uid, osv_object,
                                    fname, *args[3:], **kwargs)
                    hash =  set_hash_job(job)
                    log('Enqueued split job (%s/%s) in %s mode (id:%s): [%s] '
                        'pool(%s).%s%s' % (
                            idx + 1, len(chunks), mode, job.id,
                            dbname, osv_object, fname, tuple(args[2:])
                        )
                    )
                    jobs.append(job.id)
                return jobs
            else:
                # Remove the token
                if args[-1] == token:
                    args = args[:-1]
                return f(*args, **kwargs)
        return f_job
