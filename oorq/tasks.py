from datetime import datetime


def execute(conf_attrs, dbname, uid, obj, method, *args, **kw):
    start = datetime.now()
    # Dissabling logging in OpenERP
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
    osv_ = osv.osv.osv_pool()
    pooler.get_db_and_pool(dbname)
    logging.disable(logging.DEBUG)
    logger = logging.getLogger()
    logger.handlers = []
    logging.basicConfig()
    res = osv_.execute(dbname, uid, obj, method, *args, **kw)
    logger.info('Time elapsed: %s' % (datetime.now() - start))
    return res
