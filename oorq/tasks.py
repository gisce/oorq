# -*- coding: utf-8 -*-

from __future__ import division
import os
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime
from math import ceil

from rq import get_current_job
from rq.job import Job
from .exceptions import *
from .oorq import StoredJobsPool, setup_redis_connection, AsyncMode
from .utils import get_failed_queue


class DummySudo(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SentryCatch(object):
    def __init__(self, **kwargs):
        for _k in kwargs.keys():
            setattr(self, _k, kwargs[_k])

    def __enter__(self):
        return self

    def __getattr__(self, item):
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            import sentry_sdk
            with sentry_sdk.configure_scope() as scope:
                scope.set_tag('service_name', self.obj),
                scope.set_tag('method', self.method)
                scope.set_tag('uuid', '{}'.format(self._uuid))
                sentry_sdk.capture_exception(exc_val)


def _ensure_sys_path(path):
    """Add an ERP import path once, preserving its existing position."""
    if path not in sys.path:
        sys.path.insert(1, path)


@contextmanager
def _stack_frame(stack, value):
    """Own one stack frame and restore the exact previous stack."""
    previous = _stack_snapshot(stack)
    stack.push(value)
    try:
        yield
    finally:
        _restore_stack(stack, previous)


def _stack_snapshot(stack):
    """Return all frames, bottom first, using the LocalStack public API."""
    frames = []
    while stack.top is not None:
        frames.append(stack.pop())
    frames.reverse()
    for frame in frames:
        stack.push(frame)
    return frames


def _restore_stack(stack, frames):
    while stack.top is not None:
        stack.pop()
    for frame in frames:
        stack.push(frame)


@contextmanager
def _preserve_stack(stack):
    """Restore the exact stack after a block without adding a frame."""
    previous = _stack_snapshot(stack)
    try:
        yield
    finally:
        _restore_stack(stack, previous)


def _logging_state(logger):
    return logger.level, list(logger.handlers), logger.propagate, logger.disabled


def _restore_logging_state(logger, state):
    logger.level, handlers, logger.propagate, logger.disabled = state
    logger.handlers = handlers


def make_chunks(ids, n_chunks=None, size=None):
    """Do chunks from ids.

    We can make chunks either with number of chunks desired or size of every
    chunk.
    """
    if not n_chunks and not size:
        raise ValueError("n_chunks or size must be passed")
    if n_chunks and size:
        raise ValueError("only n_chunks or size must be passed")
    if not size:
        size = int(ceil(len(ids) / n_chunks))
    return [ids[x:x + size] for x in range(0, len(ids), size)]


def _webservice_tracker_kwargs(conf_attrs, **kwargs):
    """Propagate the server query engine snapshot to worker trackers."""
    query_engine = conf_attrs.get('global_query_engine', False)
    if query_engine:
        kwargs['ooquery_strategy'] = query_engine
    return kwargs


def execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
    start = datetime.now()
    # Disabling logging in OpenERP
    import logging
    disable_level = logging.root.manager.disable
    root_logger = logging.getLogger()
    root_state = _logging_state(root_logger)
    logger = root_logger
    logger_state = root_state
    try:
        if not os.getenv('VERBOSE', False):
            logging.disable(logging.CRITICAL)
        import netsvc
        import tools
        for attr, value in conf_attrs.items():
            tools.config[attr] = value
        _ad = os.path.abspath(os.path.join(
            tools.config['root_path'], 'addons'
        ))
        ad = os.path.abspath(tools.config['addons_path'])

        _ensure_sys_path(_ad)
        if ad != _ad:
            _ensure_sys_path(ad)
        import pooler
        from tools import config
        import osv
        import workflow
        import report
        import service
        import sql_db
        from ctx import _context_stack
        from service.security import Sudo
        from service.taskmanager import Task, TASK_CONTEXT_STACK
        from tools.service_utils import WebServiceTracker
        try:
            from tools.service_utils import SimpleGlobalUUIDGenerator
        except ImportError:
            SimpleGlobalUUIDGenerator = DummySudo

        # The worker bootstrap owns the connection pool. Replacing it for
        # every job leaks the previous pool in a persistent process.
        if getattr(sql_db, '_Pool', None) is None:
            sql_db._Pool = sql_db.ConnectionPool(
                int(tools.config['db_maxconn'])
            )
        osv_ = osv.osv.osv_pool()
        db, pool = pooler.get_db_and_pool(dbname)
        logging.disable(logging.NOTSET)
        if not pool._ready and not AsyncMode.is_async():
            logger = logging.getLogger(__name__)
            logger_state = _logging_state(logger)
        log_level = tools.config['log_level']
        worker_log_level = os.getenv('LOG', False)
        if worker_log_level:
            log_level = getattr(logging, worker_log_level, logging.INFO)
        root_logger.setLevel(log_level)
        if not pool._ready and not AsyncMode.is_async():
            logger.warning('Skipping running sync task because pool is not ready')
            return

        job_context = (_context_stack.top or {}).copy()
        context = 'sudo' in kw and Sudo(**kw.pop('sudo')) or DummySudo()
        with _stack_frame(_context_stack, job_context):
            with context:
                with SimpleGlobalUUIDGenerator() as _uuid:
                    _uuid = (
                        _uuid if not isinstance(_uuid, DummySudo) else None
                    )
                    tracker_kwargs = _webservice_tracker_kwargs(
                        conf_attrs, _uuid=_uuid, uid=uid, obj=obj,
                        method=method, db=db,
                    )
                    with WebServiceTracker(**tracker_kwargs):
                        task_id = kw.pop('current_task_id', None)
                        with _preserve_stack(TASK_CONTEXT_STACK):
                            task_context = (
                                _stack_frame(
                                    TASK_CONTEXT_STACK, Task(task_id)
                                )
                                if task_id is not None else DummySudo()
                            )
                            with task_context:
                                with SentryCatch(
                                        _uuid=_uuid, obj=obj, method=method):
                                    return osv_.execute(
                                        dbname, uid, obj, method, *args, **kw
                                    )
    finally:
        logging.disable(logging.NOTSET)
        try:
            logging.disable(logging.NOTSET)
            logger.info('Time elapsed: %s' % (datetime.now() - start))
        finally:
            if logger is not root_logger:
                _restore_logging_state(logger, logger_state)
            _restore_logging_state(root_logger, root_state)
            logging.disable(disable_level)


def isolated_execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
    if not isinstance(args[0], (tuple, list)):
        raise OORQNotIds
    start = datetime.now()
    # Disabling logging in OpenERP
    import logging
    logging.disable(logging.CRITICAL)
    import netsvc
    import tools
    for attr, value in conf_attrs.items():
        tools.config[attr] = value
    import pooler
    from tools import config
    import osv
    import workflow
    import report
    import service
    from service.security import Sudo
    from service.taskmanager import Task, TASK_CONTEXT_STACK
    from tools.service_utils import WebServiceTracker
    import sql_db
    try:
        from tools.service_utils import SimpleGlobalUUIDGenerator
    except ImportError:
        SimpleGlobalUUIDGenerator = DummySudo
    osv_ = osv.osv.osv_pool()
    pooler.get_db_and_pool(dbname)
    logging.disable(0)
    logger = logging.getLogger()
    logger.handlers = []
    log_level = tools.config['log_level']
    worker_log_level = os.getenv('LOG', False)
    if worker_log_level:
        log_level = getattr(logging, worker_log_level, 'INFO')
    logging.basicConfig(level=log_level)
    all_res = []
    failed_ids = []
    # Ensure args is a list to modify
    args = list(args)
    ids = args[0]
    context = 'sudo' in kw and Sudo(**kw.pop('sudo')) or DummySudo()
    for exe_id in ids:
        try:
            logger.info('Executing id %s' % exe_id)
            args[0] = [exe_id]
            with context:
                with SimpleGlobalUUIDGenerator() as _uuid:
                    _uuid = _uuid if not isinstance(_uuid, DummySudo) else None
                    tracker_kwargs = _webservice_tracker_kwargs(
                        conf_attrs, _uuid=_uuid, uid=uid, obj=obj,
                        method=method,
                    )
                    with WebServiceTracker(**tracker_kwargs):
                        task_pushed = False
                        if 'current_task_id' in kw:
                            task_id = kw.pop('current_task_id')
                            task = Task(task_id)
                            TASK_CONTEXT_STACK.push(task)
                            task_pushed = True
                        with SentryCatch(_uuid=_uuid, obj=obj, method=method):
                            res = osv_.execute(dbname, uid, obj, method, *args, **kw)
                        if task_pushed:
                            TASK_CONTEXT_STACK.pop()
            all_res.append(res)
        except:
            logger.error('Executing id %s failed' % exe_id)
            failed_ids.append(exe_id)
    if failed_ids:
        # Create a new job and enqueue to failed queue
        fq = get_failed_queue()
        args[0] = failed_ids
        exc_info = ''.join(traceback.format_exception(*sys.exc_info()))
        job_args = (conf_attrs, dbname, uid, obj, method) + tuple(args)
        job = Job.create(isolated_execute, job_args)
        job.origin = get_current_job().origin
        fq.add(job, exc_string=exc_info)
        logger.warning('Enqueued failed job (id:%s): [%s] pool(%s).%s%s'
                           % (job.id, dbname, obj, method, tuple(args)))
    logger.info('Time elapsed: %s' % (datetime.now() - start))

    return all_res


def report(conf_attrs, dbname, uid, obj, ids, datas=None, context=None):
    job = get_current_job()
    start = datetime.now()
    # Disabling logging in OpenERP
    import logging
    logging.disable(logging.CRITICAL)
    import netsvc
    import tools
    for attr, value in conf_attrs.items():
        tools.config[attr] = value
    import pooler
    from tools import config
    import osv
    import workflow
    import report
    import service
    import sql_db
    try:
        from tools.service_utils import SimpleGlobalUUIDGenerator
    except ImportError:
        SimpleGlobalUUIDGenerator = DummySudo
    from tools.service_utils import WebServiceTracker
    pooler.get_db_and_pool(dbname)
    logging.disable(0)
    logger = logging.getLogger()
    logger.handlers = []
    log_level = tools.config['log_level']
    worker_log_level = os.getenv('LOG', False)
    if worker_log_level:
        log_level = getattr(logging, worker_log_level, 'INFO')
    logging.basicConfig(level=log_level)
    sql_db.close_db(dbname)
    conn = sql_db.db_connect(dbname)
    cursor = conn.cursor(readonly=True, isolation_level='repeatable_read')
    _obj_name = obj
    obj = netsvc.LocalService('report.'+obj)
    if 'model' not in datas:
        datas['model'] = getattr(obj._service, 'table', False) or getattr(obj._service, 'model', False)
    with SimpleGlobalUUIDGenerator() as _uuid:
        _uuid = _uuid if not isinstance(_uuid, DummySudo) else None
        tracker_kwargs = _webservice_tracker_kwargs(
            conf_attrs, _uuid=_uuid, uid=uid, obj=_obj_name,
            method='report', db=conn,
        )
        with WebServiceTracker(**tracker_kwargs) as wst:
            with SentryCatch(_uuid=_uuid, obj=_obj_name, method='report'):
                result, format = obj.create(cursor, uid, ids, datas, context)
    job.meta['format'] = format
    job.save()
    cursor.close()
    sql_db.close_db(dbname)
    return result, format


def update_jobs_group(conf_attrs, dbname, uid, name, internal, jobs_ids):
    start = datetime.now()
    import logging
    if not os.getenv('VERBOSE', False):
        logging.disable(logging.CRITICAL)
    import netsvc
    import tools
    for attr, value in conf_attrs.items():
        tools.config[attr] = value
    _ad = os.path.abspath(os.path.join(tools.config['root_path'], 'addons'))
    ad = os.path.abspath(tools.config['addons_path'])

    sys.path.insert(1, _ad)
    if ad != _ad:
        sys.path.insert(1, ad)
    import pooler
    from tools import config
    import osv
    import workflow
    import report
    import service
    import sql_db
    # Reset the pool with config connections as limit
    sql_db._Pool = sql_db.ConnectionPool(int(tools.config['db_maxconn']))
    jobs_pool = StoredJobsPool(dbname, uid, name, internal)
    redis_conn = setup_redis_connection()
    for job_id in jobs_ids:
        jobs_pool.add_job(Job.fetch(job_id))
    jobs_pool.join()
    logging.disable(0)
    logger = logging.getLogger()
    logger.handlers = []
    log_level = tools.config['log_level']
    worker_log_level = os.getenv('LOG', False)
    if worker_log_level:
        log_level = getattr(logging, worker_log_level, 'INFO')
    logging.basicConfig(level=log_level)
    logger.info('Time elapsed: %s' % (datetime.now() - start))
    sql_db.close_db(dbname)
