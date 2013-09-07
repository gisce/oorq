# -*- coding: utf-8 -*-

from __future__ import division
import os
import sys
import traceback
from datetime import datetime
from math import ceil

from rq import get_failed_queue, get_current_job
from rq.job import Job
from exceptions import *


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
    import openerp
    for attr, value in conf_attrs.items():
        openerp.tools.config[attr] = value
    from openerp.osv.osv import object_proxy
    proxy = object_proxy()
    return proxy.execute(dbname, uid, obj, method, *args, **kw)


def isolated_execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
    import openerp
    if not isinstance(args[0], (tuple, list)):
        raise OORQNotIds
    for attr, value in conf_attrs.items():
        openerp.tools.config[attr] = value
    from openerp.osv.osv import object_proxy
    proxy = object_proxy()
    logger = openerp.netsvc.init_logger()
    all_res = []
    failed_ids = []
    # Ensure args is a list to modify
    args = list(args)
    ids = args[0]
    for exe_id in ids:
        try:
            logger.info('Executing id %s' % exe_id)
            args[0] = [exe_id]
            res = proxy.execute(dbname, uid, obj, method, *args, **kw)
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
        fq.quarantine(job, exc_info)
        logger.warning('Enqueued failed job (id:%s): [%s] pool(%s).%s%s'
                           % (job.id, dbname, obj, method, tuple(args)))
    return all_res


def report(conf_attrs, dbname, uid, obj, ids, datas=None, context=None):
    job = get_current_job()
    from openerp import tools, pooler, netsvc
    for attr, value in conf_attrs.items():
        tools.config[attr] = value
    cursor = pooler.get_db(dbname).cursor()
    obj = netsvc.LocalService('report.'+obj)
    result, format = obj.create(cursor, uid, ids, datas, context)
    job.meta['format'] = format
    job.save()
    cursor.close()
    return result, format
