# -*- coding: utf-8 -*-

from __future__ import division
import os
from datetime import datetime
from math import ceil

from rq import get_failed_queue

from exceptions import *


def make_chunks(ids, n_chunks):
    size = int(ceil(len(ids) / n_chunks))
    return [ids[x:x + size] for x in xrange(0, len(ids), size)]


def execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
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
    res = osv_.execute(dbname, uid, obj, method, *args, **kw)
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
        failed_job = fq.enqueue(isolated_execute, conf_attrs, dbname, uid, obj,
                                method, *args)
        logger.warning('Enqueued failed job (id:%s): [%s] pool(%s).%s%s'
                           % (failed_job.id, dbname, obj, method, tuple(args)))
    logger.info('Time elapsed: %s' % (datetime.now() - start))
    sql_db.close_db(dbname)
    return all_res
