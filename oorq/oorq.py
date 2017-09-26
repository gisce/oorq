# -*- coding: utf-8 -*-

from osv import osv, fields
from tools import config
from tools.translate import _
import pooler

import time
from datetime import datetime
import logging
from hashlib import sha1
from redis import Redis, from_url
from rq.job import JobStatus
from rq import Worker, Queue
from rq import cancel_job, requeue_job
from rq import push_connection, get_current_connection, local
from osconf import config_from_environment

from .utils import CursorWrapper


def oorq_log(msg, level=logging.INFO):
    logger = logging.getLogger('oorq')
    logger.log(level, msg)


def set_hash_job(job):
    """
    Assigns hash job to the job
    @param job: Rq job
    @return: hash
    """
    hash = sha1(job.get_call_string()).hexdigest()
    job.meta['hash'] = hash
    job.save()
    return hash


def get_queue(name, **kwargs):
    redis_conn = setup_redis_connection()
    kwargs['async'] = AsyncMode.is_async()
    return Queue(name, connection=redis_conn, **kwargs)


def fix_status_sync_job(job):
    """This class is for waiting the next release of RQ #640
    """
    import types

    def get_status(self):
        return JobStatus.FINISHED

    job.get_status = types.MethodType(get_status, job)


class JobsPool(object):
    def __init__(self):
        self.pending_jobs = []
        self.finished_jobs = []
        self.failed_jobs = []
        self.results = {}
        self._joined = False
        self._num_jobs = 0
        self._num_jobs_done = 0
        self.async = AsyncMode.is_async()

    @property
    def joined(self):
        return self._joined

    @joined.setter
    def joined(self, value):
        self._joined = value

    @property
    def num_jobs(self):
        return len(self.pending_jobs) + len(self.done_jobs)

    @property
    def progress(self):
        return (len(self.done_jobs) * 1.0 / self.num_jobs) * 100

    @property
    def jobs(self):
        return self.pending_jobs + self.done_jobs

    @property
    def done_jobs(self):
        return self.failed_jobs + self.finished_jobs

    def add_job(self, job):
        if self.joined:
            raise Exception("You can't add a job, the pool is joined!")
        if not self.async:
            # RQ Pull Request: #640
            fix_status_sync_job(job)
        self.pending_jobs.append(job)

    @property
    def all_done(self):
        jobs = []
        for job in self.pending_jobs:
            job_status = job.get_status()
            if job_status in (JobStatus.FINISHED, JobStatus.FAILED):
                if job_status == JobStatus.FINISHED:
                    self.finished_jobs.append(job)
                    if job.id not in self.results:
                        self.results[job.id] = job.result
                else:
                    self.failed_jobs.append(job)
            else:
                jobs.append(job)
        self.pending_jobs = jobs
        return not len(self.pending_jobs)

    def join(self):
        self.joined = True
        while not self.all_done:
            time.sleep(0.1)


def setup_redis_connection():
    redis_conn = get_current_connection()
    if not redis_conn:
        if config.get('redis_url', False):
            oorq_log('Connecting to redis using redis_url: %s'
                     % config['redis_url'])
            redis_conn = from_url(config['redis_url'])
        else:
            oorq_log('Connecting to redis using defaults')
            redis_conn = Redis()
        push_connection(redis_conn)
    return redis_conn


def serialize_date(dt):
    if not dt:
        return False
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def sql_db_dsn(db_name):
    import tools
    _dsn = ''
    for p in ('host', 'port', 'user', 'password'):
        cfg = tools.config['db_' + p]
        if cfg:
            _dsn += '%s=%s ' % (p, cfg)
    return '%sdbname=%s' % (_dsn, db_name)


def monkeypatch_sql_db_dsn():
    oorq_log("Monkeypatching sql_db.dsn...")
    import sql_db
    sql_db.dsn = sql_db_dsn


class OorqPatch(osv.osv):
    """Monkeypatching!
    """
    _name = 'oorq.patch'
    _auto = False

    def __init__(self, pool, cursor):
        monkeypatch_sql_db_dsn()
        super(OorqPatch, self).__init__(pool, cursor)

OorqPatch()


class OorqBase(osv.osv):
    """OpenObject RQ Base.
    """
    _name = 'oorq.base'
    _auto = False

    def create(self, cursor, uid, vals, context=None):
        raise osv.except_osv(
            _('Error'),
            _('You cannot create')
        )

    def unlink(self, *args, **argv):
        raise osv.except_osv(
            _('Error'),
            _('You cannot unlink')
        )

    def write(self, cursor, uid, ids, vals, context=None):
        raise osv.except_osv(
            _('Error'),
            _('You cannot write')
        )

    def search(self, cursor, uid, args, offset=0, limit=None, order=None,
               context=None, count=False):
        return [False]

OorqBase()


class OorqWorker(osv.osv):
    """OpenObject RQ Worker.
    """
    _name = 'oorq.worker'
    _inherit = 'oorq.base'
    _auto = False

    _columns = {
        'name': fields.char('Worker name', size=64),
        'queues': fields.char('Queues', size=256),
        'state': fields.char('State', size=32)
    }

    def read(self, cursor, uid, ids, fields=None, context=None):
        """Show connected workers.
        """
        setup_redis_connection()
        workers = [dict(
            id=worker.pid,
            name=worker.name,
            queues=', '.join([q.name for q in worker.queues]),
            state=worker.state,
            __last_updadate=False
        ) for worker in Worker.all()]
        return workers

OorqWorker()


class OorqQueue(osv.osv):
    """OpenObject RQ Queue.
    """
    _name = 'oorq.queue'
    _inherit = 'oorq.base'
    _auto = False

    _columns = {
        'name': fields.char('Worker name', size=64),
        'n_jobs': fields.integer('Number of jobs'),
        'is_empty': fields.boolean('Is empty'),
    }

    def read(self, cursor, uid, ids, fields=None, context=None):
        """Show connected workers.
        """
        setup_redis_connection()
        queues = [dict(
            id=i + 1,
            name=queue.name,
            n_jobs=queue.count,
            is_emprty=queue.is_empty,
            __last_updadate=False
        ) for i, queue in enumerate(Queue.all())]
        return queues

OorqQueue()


class OorqJob(osv.osv):
    """OpenObject RQ Job.
    """
    _name = 'oorq.job'
    _inherit = 'oorq.base'
    _auto = False

    _columns = {
        'jid': fields.char('Job Id', size=36),
        'queue': fields.char('Queue', size=32),
        'created_at': fields.datetime('Created at'),
        'enqueued_at': fields.datetime('Enqueued at'),
        'ended_at': fields.datetime('Ended at'),
        'origin': fields.char('Origin', size=32),
        'result': fields.text('Result'),
        'exc_info': fields.text('Exception info'),
        'description': fields.text('Description')
    }

    def cancel(self, cursor, uid, ids, context=None):
        if not context:
            context = {}
        if 'jid' in context:
            cancel_job(context['jid'])
        return True

    def requeue(self, cursor, uid, ids, context=None):
        if not context:
            context = {}
        if 'jid' in context:
            requeue_job(context['jid'])
        return True

    def read(self, cursor, uid, ids, fields=None, context=None):
        """Show connected workers.
        """
        setup_redis_connection()
        if not context:
            context = {}
        if 'queue' in context:
            queues = [Queue(context['queue'])]
        else:
            queues = Queue.all()
            try:
                queues.remove(Queue('failed'))
            except ValueError:
                pass
        jobs = []
        for qi, queue in enumerate(queues):
            jobs += [dict(
                id=int('%s%s' % (qi + 1, ji)),
                jid=job.id,
                queue=queue.name,
                created_at=serialize_date(job.created_at),
                enqueued_at=serialize_date(job.enqueued_at),
                ended_at=serialize_date(job.ended_at),
                origin=job.origin or False,
                result=job._result or False,
                exc_info=job.exc_info or False,
                description=job.description or False)
                     for ji, job in enumerate(queue.jobs)]
        return jobs

OorqJob()


class OorqJobsGroup(osv.osv):
    _name = 'oorq.jobs.group'

    _columns = {
        'name': fields.char('Name', size=256),
        'internal': fields.char('Internal name', size=256),
        'num_jobs': fields.integer('Number of Jobs'),
        'progress': fields.float('Progress'),
        'start': fields.datetime('Start'),
        'end': fields.datetime('End'),
        'active': fields.boolean('Active', select=True),
        'user_id': fields.many2one('res.users', 'User')
    }

    _defaults = {
        'start': lambda *a: datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'active': lambda *a: 1,
    }

    _order = 'start desc'

OorqJobsGroup()


class StoredJobsPool(JobsPool):

    REFRESH_INTERVAL = 5

    def __init__(self, dbname, uid, name, internal):
        self.db, self.pool = pooler.get_db_and_pool(dbname)
        self.uid = uid
        self.name = name
        self.internal = internal
        super(StoredJobsPool, self).__init__()

    def join(self):
        self.joined = True
        obj = self.pool.get('oorq.jobs.group')
        with CursorWrapper(self.db.cursor()) as wrapper:
            cursor = wrapper.cursor
            group_id = obj.create(cursor, self.uid, {
                'name': self.name,
                'internal': self.internal,
                'num_jobs': self.num_jobs,
                'user_id': self.uid
            })
            cursor.commit()

        while not self.all_done:
            with CursorWrapper(self.db.cursor()) as wrapper:
                cursor = wrapper.cursor
                obj.write(cursor, self.uid, [group_id], {
                    'progress': self.progress
                })
                cursor.commit()
            time.sleep(self.REFRESH_INTERVAL)

        with CursorWrapper(self.db.cursor()) as wrapper:
            cursor = wrapper.cursor
            obj.write(cursor, self.uid, [group_id], {
                'progress': self.progress,
                'active': 0,
                'end': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            cursor.commit()


class AsyncMode(object):

    __slots__ = ('mode', )

    def __init__(self, mode='async'):
        self.mode = mode

    def __enter__(self):
        _async_mode_stack.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _async_mode_stack.pop()

    @staticmethod
    def is_async():
        async_mode = _async_mode_stack.top
        if async_mode is None:
            async_env = config_from_environment('OORQ').get('async', True)
            return async_env
        else:
            return async_mode.mode == 'async'


_async_mode_stack = local.LocalStack()
