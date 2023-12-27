# -*- coding: utf-8 -*-

from __future__ import division
import os
import sys
import traceback
from datetime import datetime
from math import ceil

from rq import get_current_job
from rq.job import Job
from .exceptions import *
from .oorq import StoredJobsPool, setup_redis_connection, AsyncMode
from .utils import get_failed_queue


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
    return [ids[x:x + size] for x in xrange(0, len(ids), size)]


def execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
    start = datetime.now()
    # Disabling logging in OpenERP
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
    from ctx import _context_stack
    # Reset the pool with config connections as limit
    sql_db._Pool = sql_db.ConnectionPool(int(tools.config['db_maxconn']))
    osv_ = osv.osv.osv_pool()
    db, pool = pooler.get_db_and_pool(dbname)
    logging.disable(0)
    if not pool._ready and not AsyncMode.is_async():
        logger = logging.getLogger(__name__)
    else:
        logger = logging.getLogger()
    logger.handlers = []
    log_level = tools.config['log_level']
    worker_log_level = os.getenv('LOG', False)
    if worker_log_level:
        log_level = getattr(logging, worker_log_level, 'INFO')
    logging.basicConfig(level=log_level)
    if not pool._ready and not AsyncMode.is_async():
        logger.warning('Skipping running sync task because pool is not ready')
        return
    if _context_stack.top is None:
        _context_stack.push({})
    res = osv_.execute(dbname, uid, obj, method, *args, **kw)
    _context_stack.pop()
    logger.info('Time elapsed: %s' % (datetime.now() - start))
    sql_db.close_db(dbname)
    return res


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
    import sql_db
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
    for exe_id in ids:
        try:
            logger.info('Executing id %s' % exe_id)
            args[0] = [exe_id]
            res = osv_.execute(dbname, uid, obj, method, *args, **kw)
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
    sql_db.close_db(dbname)
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
    cursor = conn.cursor()
    obj = netsvc.LocalService('report.'+obj)
    if 'model' not in datas:
        datas['model'] = getattr(obj._service, 'table', False)
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
