# -*- coding: utf-8 -*-

from osv import osv, fields
from tools import config
from tools.translate import _
import pooler

import time
import inspect
from datetime import datetime, timedelta
import logging
from hashlib import sha1
from redis import Redis, from_url
from pytz import timezone
from rq.job import JobStatus, Job
from rq import Worker, Queue, registry
from rq import cancel_job, requeue_job
from rq import push_connection, get_current_connection, local
from osconf import config_from_environment
from six import text_type

from .utils import CursorWrapper
import os


REGISTRY_MAP = {
    'failed': registry.FailedJobRegistry,
    'started': registry.StartedJobRegistry,
    'finished': registry.FinishedJobRegistry,
    'deferred': registry.DeferredJobRegistry
}


def oorq_log(msg, level=logging.INFO):
    logger = logging.getLogger('openerp.oorq')
    logger.log(level, msg)


def set_hash_job(job):
    """
    Assigns hash job to the job
    @param job: Rq job
    @return: hash
    """
    call_string = job.get_call_string()
    if isinstance(call_string, text_type):
        call_string = call_string.encode('utf-8')
    hash = sha1(call_string).hexdigest()
    job.meta['hash'] = hash
    job.save()
    return hash


def get_queue(name, **kwargs):
    redis_conn = setup_redis_connection()
    kwargs['is_async'] = AsyncMode.is_async()
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
        self.is_async = AsyncMode.is_async()

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
        if not self.is_async:
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


class ProgressJobsPool(JobsPool):

    def __init__(self, browse_obj=None, browse_obj_proggress_field=None, logger_description="openerp.task.progress"):
        self.browse_obj = browse_obj
        self.progress_field = browse_obj_proggress_field
        self.logger_description = logger_description
        super(ProgressJobsPool, self).__init__()

    @property
    def all_done(self):
        logger = logging.getLogger(self.logger_description)
        logger.info('Progress: {0}/{1} {2}%'.format(
            len(self.done_jobs), self.num_jobs, self.progress
        ))
        if self.progress_field and self.browse_obj:
            self.browse_obj.write({self.progress_field: self.progress})
        return super(ProgressJobsPool, self).all_done


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
    ssl_redis_connection = config.get('ssl_redis_connection', False) or False
    autoworker_redis_url = config.get('autoworker_redis_url', False) or False
    os.environ['AUTOWORKER_REDIS_URL'] = (
            autoworker_redis_url or get_redis_url(redis_conn, ssl_redis_connection=ssl_redis_connection)
    )
    return redis_conn


def get_redis_url(redis_conn, ssl_redis_connection=False):
    """
    Creates redis url from redis connection
    :param redis_conn:
    :param ssl_redis_connection:
    :return: url on the form redis://host:port/db
    """
    if not redis_conn:
        return False

    if ssl_redis_connection:
        prefix = 'rediss'
    else:
        prefix = 'redis'

    return '{}://{host}:{port}/{db}'.format(
        prefix, **redis_conn.connection_pool.connection_kwargs
    )

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
        'wclass': fields.char('Worker class', size=64),
        'queues': fields.char('Queues', size=256),
        'state': fields.char('State', size=32),
        'total_working_time': fields.char('Total working time', size=32),
        'successful_job_count': fields.integer('Successful job count'),
        'failed_job_count': fields.integer('Failed job count'),
        'last_heartbeat': fields.datetime('Last heartbeat'),
        'birth_date': fields.datetime('Birth date'),
        'current_job_id': fields.char('Current Job Id', size=36)
    }

    def read(self, cursor, uid, ids, fields=None, context=None, load='_classic_read'):
        """Show connected workers.
        """
        setup_redis_connection()
        user = self.pool.get('res.users').read(cursor, uid, uid, ['context_tz'])
        tz = timezone(user['context_tz'] or 'UTC')
        workers = [dict(
            id=worker.pid,
            name=worker.name,
            wclass='{}.{}'.format(worker.__module__, worker.__class__.__name__),
            queues=', '.join([q.name for q in worker.queues]),
            state=worker.state,
            total_working_time=str(timedelta(
                microseconds=worker.total_working_time
            )),
            successful_job_count=worker.successful_job_count,
            failed_job_count=worker.failed_job_count,
            last_heartbeat=worker.last_heartbeat and worker.last_heartbeat.replace(
                tzinfo=timezone('UTC')
            ).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S') or False,
            birth_date=worker.birth_date and worker.birth_date.replace(
                tzinfo=timezone('UTC')
            ).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S') or False,
            current_job_id=worker.get_current_job_id() or False,
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
        'name': fields.char('Queue name', size=64),
        'n_jobs': fields.integer('Number of jobs'),
        'is_empty': fields.boolean('Is empty'),
    }

    def read(self, cursor, uid, ids, fields=None, context=None, load='_classic_read'):
        """Show connected workers.
        """
        setup_redis_connection()
        if fields is None:
            fields = []
        queues = [queue for queue in Queue.all() if getattr(queue, 'name') in ids]

        res = []
        for queue in queues:
            values = {
                'id': queue.name,
                'name': queue.name,
                'n_jobs': len(queue.jobs),
                'is_empty': queue.is_empty()
            }
            for key in list(values.keys()):
                if key not in fields and key != 'id':
                    values.pop(key, None)
            res.append(values)

        return res

    def search(self, cursor, uid, args, offset=0, limit=None, order=None,
               context=None, count=False):
        setup_redis_connection()
        res = Queue.all()

        for arg in args:
            if arg:
                column, op, value = arg
                if op in ['=', '!=']:
                    res = [queue for queue in res if (getattr(queue, column) == value) != ('!' in op)]
                elif op == 'ilike':
                    res = [queue for queue in res if str(value).lower() in str(getattr(queue, column)).lower()]
                elif op == 'like':
                    res = [queue for queue in res if value in getattr(queue, column)]
                elif op in ['in', 'not in']:
                    res = [queue for queue in res if (getattr(queue, 'name') in value) != ('not' in op)]

        res = res[int(offset):]
        if limit:
            res = res[:limit]

        if count:
            return len(res)
        else:
            return [getattr(queue, 'name') for queue in res]

OorqQueue()


class OorqRegistry(osv.osv):
    _name = 'oorq.registry'
    _inherit = 'oorq.base'
    _auto = False
    registry = None

    _columns = {
        'name': fields.char('Registry name', size=64),
        'n_jobs': fields.integer('Number of jobs'),
        'queue': fields.char('Queue name', size=64),
    }

    def read(self, cursor, uid, ids, fields=None, context=None, load='_classic_read'):
        """Show connected workers.
        """
        setup_redis_connection()
        registries = []
        for idx, queue in enumerate(Queue.all()):
            reg = self.registry(name=queue.name)
            assert isinstance(reg, registry.BaseRegistry)
            registries.append({
                'id': idx,
                'name': reg.name,
                'n_jobs': reg.count,
                'queue': reg.name
            })
        return registries


OorqRegistry()


class OorqRegistryFailed(osv.osv):
    _name = 'oorq.registry.failed'
    _auto = False
    _inherit = 'oorq.registry'
    registry = registry.FailedJobRegistry


OorqRegistryFailed()


class OorqRegistryStarted(osv.osv):
    _name = 'oorq.registry.started'
    _auto = False
    _inherit = 'oorq.registry'
    registry = registry.StartedJobRegistry


OorqRegistryStarted()


class OorqRegistryFinished(osv.osv):
    _name = 'oorq.registry.finished'
    _auto = False
    _inherit = 'oorq.registry'
    registry = registry.FinishedJobRegistry


OorqRegistryFinished()


class OorqRegistryDeferred(osv.osv):
    _name = 'oorq.registry.deferred'
    _auto = False
    _inherit = 'oorq.registry'
    registry = registry.DeferredJobRegistry


OorqRegistryDeferred()


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
        'description': fields.text('Description'),
        'status': fields.selection([
            ('queued', 'Queued'),
            ('finished', 'Finished'),
            ('failed', 'Failed'),
            ('started', 'Started'),
            ('deferred', 'Deferred')
        ], 'Status')
    }

    def cancel(self, cursor, uid, ids, context=None):
        if not context:
            context = {}
        if 'jid' in context:
            job = Job.fetch(context['jid'])
            oorq_log('Canceling job {}'.format(context['jid']))
            job.cancel()
            job.delete()
        return True

    def requeue(self, cursor, uid, ids, context=None):
        if not context:
            context = {}
        conn = setup_redis_connection()
        if 'jid' in context:
            requeue_job(context['jid'], connection=conn)
        return True

    def read(self, cursor, uid, ids, fields=None, context=None, load='_classic_read'):
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
        result = []
        user = self.pool.get('res.users').read(cursor, uid, uid, ['context_tz'])
        tz = timezone(user['context_tz'] or 'UTC')
        for qi, queue in enumerate(queues):
            if 'registry' in context:
                reg = REGISTRY_MAP.get(context['registry'])
                if reg is None:
                    raise osv.except_osv('Error', 'Regitry {} not valid'.format(
                        context['registry']
                    ))
                jobs = [Job.fetch(jid) for jid in reg(queue.name).get_job_ids()]
            else:
                jobs = queue.jobs
            result += [dict(
                id=int('%s%s' % (qi + 1, ji)),
                jid=job.id,
                queue=queue.name,
                created_at=job.created_at and serialize_date(
                    job.created_at.replace(
                        tzinfo=timezone('UTC')
                    ).astimezone(tz)
                ),
                enqueued_at=job.enqueued_at and serialize_date(
                    job.enqueued_at.replace(
                        tzinfo=timezone('UTC')
                    ).astimezone(tz)
                ),
                ended_at=job.ended_at and serialize_date(
                    job.ended_at.replace(
                        tzinfo=timezone('UTC')
                    ).astimezone(tz)
                ),
                origin=job.origin or False,
                result=str(job._result) or False,
                exc_info=job.exc_info or False,
                status=job.get_status(),
                description=job.description or False)
                     for ji, job in enumerate(jobs)]
        return result


OorqJob()


class OorqJobsGroup(osv.osv):
    _name = 'oorq.jobs.group'

    _columns = {
        'name': fields.char('Name', size=256),
        'internal': fields.char('Internal name', size=256),
        'num_failed_jobs': fields.integer('Number of failed Jobs'),
        'num_success_jobs': fields.integer('Number of success Jobs'),
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
                    'progress': self.progress,
                    'num_failed_jobs': len(self.failed_jobs),
                    'num_success_jobs': len(self.finished_jobs)
                })
                cursor.commit()
            time.sleep(self.REFRESH_INTERVAL)

        with CursorWrapper(self.db.cursor()) as wrapper:
            cursor = wrapper.cursor
            obj.write(cursor, self.uid, [group_id], {
                'progress': self.progress,
                'active': 0,
                'num_failed_jobs': len(self.failed_jobs),
                'num_success_jobs': len(self.finished_jobs),
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
