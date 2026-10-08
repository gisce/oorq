from copy import copy
from datetime import datetime
from logging import Filter
import os
import resource

from rq import Worker as RQWorker
from rq.worker import SimpleWorker as RQSimpleWorker
from rq.job import Job as RQJob
from six import string_types
try:
    from rq.exceptions import DeserializationError
except ImportError:
    from rq.exceptions import UnpickleError as DeserializationError
import sys


CONFIG_TASKS = (
    'oorq.tasks.execute', 'oorq.tasks.isolated_execute',
    'oorq.tasks.report', 'oorq.tasks.update_jobs_group',
)
PERSISTENT_WORKER_CONFIG = 'oorq_persistent_worker'
PERSISTENT_MAX_JOBS_CONFIG = 'oorq_persistent_max_jobs'
DEFAULT_PERSISTENT_MAX_JOBS = 100
TRUE_CONFIG_VALUES = (True, 1, '1', 'true', 'yes', 'on')
FALSE_CONFIG_VALUES = (False, 0, None, '', '0', 'false', 'no', 'off')


def _call_unbound(method, instance, *args):
    """Call an RQ implementation with a compatible Python 2/3 binding."""
    function = getattr(method, 'im_func', method)
    return function(instance, *args)


def _pubsub_subscriptions(config, queues):
    dbname = config['db_name']
    subscriptions = list(config.pubsub_subscriptions)
    subscriptions.append('{}.worker'.format(dbname))
    subscriptions.extend(
        '{}.worker.{}'.format(dbname, queue.name) for queue in queues
    )
    unique = []
    for subscription in subscriptions:
        if subscription not in unique:
            unique.append(subscription)
    return unique


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

    def _configured_max_jobs(self, config):
        value = config.options.get(
            PERSISTENT_MAX_JOBS_CONFIG, DEFAULT_PERSISTENT_MAX_JOBS
        )
        if value in (None, False, ''):
            return None
        try:
            value = int(value)
        except (TypeError, ValueError):
            self.log.warning(
                'Invalid %s value %r; using %d',
                PERSISTENT_MAX_JOBS_CONFIG, value,
                DEFAULT_PERSISTENT_MAX_JOBS,
            )
            return DEFAULT_PERSISTENT_MAX_JOBS
        if value < 0:
            self.log.warning(
                'Invalid %s value %r; using %d',
                PERSISTENT_MAX_JOBS_CONFIG, value,
                DEFAULT_PERSISTENT_MAX_JOBS,
            )
            return DEFAULT_PERSISTENT_MAX_JOBS
        return value or None

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
        self.persistent_max_jobs = self._configured_max_jobs(tools.config)
        self._persistent_job_ordinal = 0
        self._persistent_exit_reason = None
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
                subscriptions = _pubsub_subscriptions(config, self.queues)
                PubSub.connect(subscriptions)
            else:
                PubSub.connect('{}.worker'.format(config['db_name']))
            from erp_sentry.sentry_base import SentryService
            SentryService()

        except ImportError:
            pass

    def work(self, *args, **kwargs):
        """Apply the configured recycle limit unless the CLI overrides it."""
        if self.persistent:
            args = list(args)
            if len(args) >= 5:
                if args[4] is None:
                    args[4] = self.persistent_max_jobs
            elif kwargs.get('max_jobs') is None:
                kwargs['max_jobs'] = self.persistent_max_jobs
            args = tuple(args)
            if self.persistent_max_jobs:
                self.log.info(
                    'Persistent worker recycle limit: %d jobs',
                    kwargs.get('max_jobs', args[4] if len(args) >= 5 else
                               self.persistent_max_jobs),
                )
        return super(ERPWorkerMixin, self).work(*args, **kwargs)

    def perform_job(self, *args, **kwargs):
        succeeded = super(ERPWorkerMixin, self).perform_job(*args, **kwargs)
        from .process_state import consume_recycle_reason
        recycle_reason = consume_recycle_reason()
        if self.persistent and (succeeded is False or recycle_reason):
            # RQ handles and records the failure before returning False.  Do
            # not reserve another job in a process that may have timed out or
            # whose application cleanup may have failed.
            self._persistent_exit_reason = recycle_reason or 'job_failed'
            self._stop_requested = True
            self.log.warning(
                'Persistent worker marked for recycle: %s',
                self._persistent_exit_reason,
            )
        return succeeded

    def _execute_persistent_job(self, implementation, job, queue):
        self._persistent_job_ordinal += 1
        ordinal = self._persistent_job_ordinal
        started = datetime.now()
        rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        try:
            return _call_unbound(implementation, self, job, queue)
        finally:
            rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            self.log.info(
                'oorq persistent job pid=%d job_id=%s ordinal=%d '
                'duration_seconds=%.6f rss_before_kb=%d rss_after_kb=%d '
                'recycle_reason=%s',
                os.getpid(), getattr(job, 'id', '<unknown>'), ordinal,
                (datetime.now() - started).total_seconds(), rss_before,
                rss_after, self._persistent_exit_reason or 'none',
            )

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

    def execute_job(self, job, queue):
        return self._execute_persistent_job(
            RQSimpleWorker.execute_job, job, queue
        )


class Worker(NoPersistentWorker):
    """Stable CLI entry point selecting its execution strategy after bootstrap.

    The object remains an RQ ``Worker`` (and a ``NoPersistentWorker``) so RQ's
    lifecycle and introspection stay stable.  Only the execution method is
    selected late, once the ERP configuration has been parsed.
    """

    select_worker_from_config = True

    def execute_job(self, job, queue):
        if self.persistent:
            return self._execute_persistent_job(
                RQSimpleWorker.execute_job, job, queue
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
