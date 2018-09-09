# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
from multiprocessing import cpu_count
import os

from rq import Queue
from rq.job import Job
from rq import get_current_job
from .oorq import setup_redis_connection, set_hash_job, AsyncMode, get_redis_url
from osconf import config_from_environment
from .exceptions import *

from .tasks import make_chunks, execute, isolated_execute, update_jobs_group
from tools import config
import netsvc
from signals import DB_CURSOR_COMMIT, DB_CURSOR_ROLLBACK
from autoworker import AutoWorker


class ProcessJobs(object):

    JOBS_TO_PROCESS = {}

    @classmethod
    def add_job(cls, transaction_id, job, queue):
        cls.JOBS_TO_PROCESS.setdefault(transaction_id, [])
        cls.JOBS_TO_PROCESS[transaction_id].append(
            (job, queue)
        )

    @staticmethod
    def commit(cursor):
        transaction_id = id(cursor)
        jobs = ProcessJobs.JOBS_TO_PROCESS.pop(transaction_id, [])
        for job, queue in jobs:
            queue.enqueue_job(job)
            log('Enqueued job {} to queue {} from commit transaction {}'.format(
                job.id, queue.name, transaction_id
            ))

    @staticmethod
    def rollback(cursor):
        transaction_id = id(cursor)
        jobs = ProcessJobs.JOBS_TO_PROCESS.pop(transaction_id, [])
        if jobs:
            log('Cancelling {} jobs from rollback of transaction {}'.format(
                len(jobs), transaction_id
            ))


DB_CURSOR_COMMIT.connect(ProcessJobs.commit)
DB_CURSOR_ROLLBACK.connect(ProcessJobs.rollback)


def log(msg, level=netsvc.LOG_INFO):
    logger = netsvc.Logger()
    logger.notifyChannel('oorq', level, msg)


class job(object):
    def __init__(self, *args, **kwargs):
        self.async = AsyncMode.is_async()
        self.queue = 'default'
        self.timeout = None
        self.result_ttl = None
        self.at_front = False
        # Assign all the arguments to attributes
        config = config_from_environment('OORQ', **kwargs)
        for arg, value in config.items():
            setattr(self, arg, value)

    def __call__(self, f):
        token = sha1(f.__name__).hexdigest()

        def f_job(*args, **kwargs):
            redis_conn = setup_redis_connection()
            current_job = get_current_job()
            async_mode = AsyncMode.is_async()
            if not args[-1] == token:
                # Add the token as a last argument
                args += (token,)
                # Default arguments
                osv_object = args[0]._name
                cursor = args[1]
                dbname = cursor.dbname
                uid = args[2]
                fname = f.__name__
                q = Queue(self.queue, default_timeout=self.timeout,
                          connection=redis_conn, async=async_mode)
                # Pass OpenERP server config to the worker
                conf_attrs = dict(
                    [(attr, value) for attr, value in config.options.items()]
                )
                job_args = (
                    conf_attrs, dbname, uid, osv_object, fname
                ) + args[3:]
                job_kwargs = kwargs
                job = Job.create(
                    execute,
                    args=job_args,
                    kwargs=job_kwargs,
                    result_ttl=self.result_ttl,
                    depends_on=current_job
                    at_front=self.at_front
                )
                set_hash_job(job)
                transaction_id = id(cursor)
                ProcessJobs.add_job(transaction_id, job, q)
                log('Created job (id:%s) for queue %s: [%s] pool(%s).%s%s '
                    '(waiting to commit/rollback %s)' % (
                    job.id, q.name, dbname, osv_object, fname, args[2:],
                    transaction_id
                ))
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
            redis_conn = setup_redis_connection()
            current_job = get_current_job()
            async = self.async and AsyncMode.is_async()
            if not args[-1] == token and async:
                # Add the token as a last argument
                args += (token,)
                # Default arguments
                osv_object = args[0]._name
                cursor = args[1]
                dbname = cursor.dbname
                uid = args[2]
                ids = args[3]
                if not isinstance(ids, (list, tuple)):
                    raise OORQNotIds()
                
                fname = f.__name__
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
                    job_args = [
                        conf_attrs, dbname, uid, osv_object, fname
                    ] + args[3:]
                    job_kwargs = kwargs
                    job = Job.create(
                        task,
                        depends_on=current_job,
                        result_ttl=self.result_ttl,
                        args=job_args,
                        kwargs=job_kwargs
                    )
                    set_hash_job(job)
                    transaction_id = id(cursor)
                    ProcessJobs.add_job(transaction_id, job, q)
                    log('Created split job (%s/%s) on queue %s in %s mode '
                        '(id:%s): [%s] pool(%s).%s%s '
                        '(waiting to commit/rollback %s)' % (
                            idx + 1, len(chunks), q.name, mode, job.id,
                            dbname, osv_object, fname, tuple(args[2:]),
                            transaction_id
                        )
                    )
                    jobs.append(job)
                return jobs
            else:
                # Remove the token
                if args[-1] == token:
                    args = args[:-1]
                return f(*args, **kwargs)
        return f_job


def create_jobs_group(dbname, uid, name, internal, jobs_ids):
    conf_attrs = dict(
        [(attr, value) for attr, value in config.options.items()]
    )
    conn = setup_redis_connection()
    queue = 'jobspool-autoworker'
    q = Queue(queue, default_timeout=3600 * 24, connection=conn)
    enqueued_job = q.enqueue(
        update_jobs_group, conf_attrs, dbname, uid, name, internal, jobs_ids
    )
    if not os.environ['AUTOWORKER_REDIS_URL']:
        os.environ['AUTOWORKER_REDIS_URL'] = get_redis_url(conn)
    aw = AutoWorker(queue, max_procs=1)
    aw.work()
    return enqueued_job
