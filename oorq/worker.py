from rq import Worker as RQWorker
import sys
try:
    from signals import WORKER_STARTED
except ImportError:
    WORKER_STARTED = None


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
        netsvc.init_logger()
        netsvc.SERVICES['im_a_worker'] = True
        self.log.propagate = False
        try:
            from service.pubsub import PubSub
            _queue_names = [_q.name for _q in self.queues]
            if hasattr(tools.config, 'pubsub_subscriptions'):
                subscriptions = (
                    config.pubsub_subscriptions +
                    ['{}.worker'.format(config['db_name'])] +
                    [
                        '{}.worker.{}'.format(config['db_name'], _qn)
                        for _qn in _queue_names
                    ]
                )
                PubSub.connect(subscriptions)
            else:
                PubSub.connect('{}.worker'.format(config['db_name']))
            if WORKER_STARTED is not None:
                WORKER_STARTED.send(queues=_queue_names)
            from erp_sentry.sentry_base import SentryService
            SentryService()

        except ImportError:
            pass

    def request_stop(self, signum, frame):
        try:
            from signals import SHUTDOWN_REQUEST
            SHUTDOWN_REQUEST.send(signum, frame=frame, exit_code=0)
        except TypeError:
            # Backwards compatible
            SHUTDOWN_REQUEST.send(signum, frame=frame)
        except ImportError:
            pass
        super(Worker, self).request_stop(signum, frame)
