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
        try:
            from service.pubsub import PubSub
            PubSub.connect('{}.worker'.format(config['db_name']))
        except ImportError:
            pass

    def request_stop(self, signum, frame):
        try:
            from service import kill_current_threads
            kill_current_threads(signum, frame)
        except ImportError:
            pass
        super(Worker, self).request_stop(signum, frame)
