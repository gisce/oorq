def execute(addons_path, dbname, uid, obj, method, *args, **kw):
    import logging
    logging.disable(logging.CRITICAL)
    import netsvc
    import tools
    tools.config['db_name'] = dbname
    tools.config['addons_path'] = addons_path
    import pooler
    from tools import config
    import osv
    import workflow
    import report
    import service
    osv_ = osv.osv.osv_pool()
    return osv_.execute(dbname, uid, obj, method, *args, **kw)