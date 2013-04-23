# -*- coding: utf-8 -*-

from osv import osv, fields
from tools.translate import _

from redis import Redis
from rq import Worker, Queue
from rq import push_connection


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
        redis_conn = Redis()
        push_connection(redis_conn)
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
        'queues': fields.char('Queues', size=256),
        'state': fields.char('State', size=32)
    }

    def read(self, cursor, uid, ids, fields=None, context=None):
        """Show connected workers.
        """
        redis_conn = Redis()
        push_connection(redis_conn)
        queues = [dict(
            id=i + 1,
            name=queue.name,
            n_jobs=queue.count,
            __last_updadate=False
        ) for i, queue in enumerate(Queue.all())]
        return queues

OorqQueue()
