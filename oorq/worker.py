from rq import Worker as RQWorker
import sys


class Worker(RQWorker):

    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        sys.argv = sys.argv[:1]
        import netsvc
        import tools
        tools.config.parse()
        import pooler
        from tools import config
        import osv
        import workflow
        import report
        import service
        import sql_db
        osv_ = osv.osv.osv_pool()
        pooler.get_db_and_pool(config['db_name'])
        netsvc.SERVICES['im_a_worker'] = True
        self.log.propagate = False