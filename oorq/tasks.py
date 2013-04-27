import os
from datetime import datetime

from exceptions import *


def make_chunks(ids, size):
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
    ids = args[0]
    for exe_id in ids:
        try:
            args[0] = [exe_id]
            res = osv_.execute(dbname, uid, obj, method, *args, **kw)
            all_res.append(res)
        except:
            logger.error('Execute failed: [%s] %s.%s%s' % (dbname, obj, args))
            failed_ids.append(exe_id)
            # Create a new job and enqueue to failed queue
    logger.info('Time elapsed: %s' % (datetime.now() - start))
    sql_db.close_db(dbname)
    return res
