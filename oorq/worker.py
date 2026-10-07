from copy import copy
from logging import Filter

from rq import Worker as RQWorker
from rq.worker import SimpleWorker as RQSimpleWorker
from rq.job import Job as RQJob
try:
    from rq.exceptions import DeserializationError
except ImportError:
    from rq.exceptions import UnpickleError as DeserializationError
import sys

try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)


CONFIG_TASKS = (
    'oorq.tasks.execute', 'oorq.tasks.isolated_execute',
    'oorq.tasks.report', 'oorq.tasks.update_jobs_group',
)
PERSISTENT_WORKER_CONFIG = 'oorq_persistent_worker'
TRUE_CONFIG_VALUES = (True, 1, '1', 'true', 'yes', 'on')
FALSE_CONFIG_VALUES = (False, 0, None, '', '0', 'false', 'no', 'off')


def _call_unbound(method, instance, *args):
    """Call an RQ implementation with a compatible Python 2/3 binding."""
    function = getattr(method, 'im_func', method)
    return function(instance, *args)


class WorkerLogFilter(Filter):
    def filter(self, record):
        # RQ also attaches the raw arguments to exception log records.
        if getattr(record, 'func', None) in CONFIG_TASKS:
            record.arguments = record.arguments[1:]
            record.kwargs = dict(
                (key, value) for key, value in record.kwargs.items()
                if key != 'conf_attrs'
            )
        return True


class WorkerJob(RQJob):
    """Keep server configuration out of descriptions logged by RQ."""

    def restore(self, raw_data):
        super(WorkerJob, self).restore(raw_data)
        try:
            if self.func_name not in CONFIG_TASKS:
                return
            # Preserve execution data and hashes; only change the displayed call.
            displayed_job = copy(self)
            displayed_job.args = self.args[1:]
            displayed_job.kwargs = dict(
                (key, value) for key, value in self.kwargs.items()
                if key != 'conf_attrs'
            )
            self.description = displayed_job.get_call_string()
        except DeserializationError:
            # The saved description may contain configuration too.
            self.description = '<DeserializationError>'


class ERPWorkerMixin(object):

    job_class = WorkerJob
    log_filter = WorkerLogFilter()
    persistent = False
    select_worker_from_config = False

    def __init__(self, *args, **kwargs):
        super(ERPWorkerMixin, self).__init__(*args, **kwargs)
        # The RQ CLI explicitly passes its default Job class.
        if self.job_class is RQJob:
            self.job_class = WorkerJob
        self.log.addFilter(self.log_filter)
        sys.argv = sys.argv[:1]
        import netsvc
        import tools
        tools.config.parse()
        self.persistent = self._persistent_worker_enabled(tools.config)
        effective_worker = (
            'PersistentWorker' if self.persistent else 'NoPersistentWorker'
        )
        self.log.info(
            'oorq worker strategy: %s (%s=%r)',
            effective_worker,
            PERSISTENT_WORKER_CONFIG,
            tools.config.options.get(PERSISTENT_WORKER_CONFIG),
        )
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
            if hasattr(tools.config, 'pubsub_subscriptions'):
                subscriptions = (
                    config.pubsub_subscriptions +
                    ['{}.worker'.format(config['db_name'])] +
                    [
                        '{}.worker.{}'.format(config['db_name'], _q.name)
                        for _q in self.queues
                    ]
                )
                PubSub.connect(subscriptions)
            else:
                PubSub.connect('{}.worker'.format(config['db_name']))
            from erp_sentry.sentry_base import SentryService
            SentryService()

        except ImportError:
            pass

    def _persistent_worker_enabled(self, config):
        if not self.select_worker_from_config:
            return self.persistent

        value = config.options.get(PERSISTENT_WORKER_CONFIG)
        if isinstance(value, string_types):
            value = value.strip().lower()
        enabled = value in TRUE_CONFIG_VALUES
        if value not in TRUE_CONFIG_VALUES + FALSE_CONFIG_VALUES:
            self.log.warning(
                'Invalid %s value %r; using NoPersistentWorker',
                PERSISTENT_WORKER_CONFIG,
                value,
            )
        return enabled

    def request_stop(self, signum, frame):
        try:
            from signals import SHUTDOWN_REQUEST
            SHUTDOWN_REQUEST.send(signum, frame=frame, exit_code=0)
        except TypeError:
            # Backwards compatible
            SHUTDOWN_REQUEST.send(signum, frame=frame)
        except ImportError:
            pass
        super(ERPWorkerMixin, self).request_stop(signum, frame)


class NoPersistentWorker(ERPWorkerMixin, RQWorker):
    """Worker that preserves RQ's forked work horse isolation."""


class PersistentWorker(ERPWorkerMixin, RQSimpleWorker):
    """Opt-in worker that executes consecutive jobs in the same process."""

    persistent = True


class Worker(NoPersistentWorker):
    """Stable CLI entry point selecting its execution strategy after bootstrap.

    The object remains an RQ ``Worker`` (and a ``NoPersistentWorker``) so RQ's
    lifecycle and introspection stay stable.  Only the execution method is
    selected late, once the ERP configuration has been parsed.
    """

    select_worker_from_config = True

    def execute_job(self, job, queue):
        if self.persistent:
            return _call_unbound(
                RQSimpleWorker.execute_job, self, job, queue
            )
        return _call_unbound(
            NoPersistentWorker.execute_job, self, job, queue
        )

if 'get_heartbeat_ttl' in RQSimpleWorker.__dict__:
    def get_heartbeat_ttl(self, job):
        if self.persistent:
            return _call_unbound(
                RQSimpleWorker.get_heartbeat_ttl, self, job
            )
        return _call_unbound(RQWorker.get_heartbeat_ttl, self, job)

    Worker.get_heartbeat_ttl = get_heartbeat_ttl
