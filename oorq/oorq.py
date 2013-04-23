# -*- coding: utf-8 -*-

import netsvc
from osv import osv, fields
from tools.translate import _

from redis import Redis
from rq import Worker, Queue
from rq import push_connection, get_current_connection


def oorq_log(msg, level=netsvc.LOG_INFO):
    logger = netsvc.Logger()
    logger.notifyChannel('oorq', level, msg)


def setup_redis_connection():
    if not get_current_connection():
        redis_conn = Redis()
        push_connection(redis_conn)


def serialize_date(dt):
    if not dt:
        return False
    return dt.strftime('%Y-%m-%d %H:%M:%S')


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
