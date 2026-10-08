from rq import Worker as RQWorker
from rq.worker import SimpleWorker
import sys

from oorq.worker import (
    DEFAULT_PERSISTENT_MAX_JOBS, ERPWorkerMixin, NoPersistentWorker,
    PersistentWorker, Worker, WorkerJob, _pubsub_subscriptions,
)
from oorq.process_state import request_recycle


def function(method):
    return getattr(method, 'im_func', method)


def test_default_worker_keeps_fork_based_rq_worker():
    assert issubclass(Worker, ERPWorkerMixin)
    assert issubclass(Worker, NoPersistentWorker)
    assert issubclass(Worker, RQWorker)
    assert Worker.job_class is WorkerJob


def test_persistent_worker_uses_simple_worker_execution():
    assert issubclass(PersistentWorker, ERPWorkerMixin)
    assert issubclass(PersistentWorker, SimpleWorker)
    assert PersistentWorker.job_class is WorkerJob


def worker_with_config(value=None, present=True):
    worker = Worker.__new__(Worker)
    worker.select_worker_from_config = True
    worker.persistent = False
    worker.log = type('Log', (), {
        'warning': lambda *args: None,
        'info': lambda *args: None,
    })()
    options = {}
    if present:
        options['oorq_persistent_worker'] = value
    config = type('Config', (), {'options': options})()
    worker.persistent = worker._persistent_worker_enabled(config)
    worker._persistent_job_ordinal = 0
    worker._persistent_exit_reason = None
    return worker


def persistent_worker_state():
    worker = PersistentWorker.__new__(PersistentWorker)
    worker.persistent = True
    worker._stop_requested = False
    worker._persistent_exit_reason = None
    worker.log = type('Log', (), {
        'warning': lambda *args: None,
        'info': lambda *args: None,
    })()
    return worker


def test_worker_defaults_to_non_persistent_strategy():
    assert worker_with_config(present=False).persistent is False


def test_worker_uses_non_persistent_strategy_when_explicitly_disabled():
    assert worker_with_config(False).persistent is False
    assert worker_with_config('off').persistent is False


def test_worker_uses_persistent_strategy_when_enabled():
    assert worker_with_config(True).persistent is True
    assert worker_with_config('yes').persistent is True


def test_worker_rejects_invalid_persistent_strategy_value():
    assert worker_with_config('definitely').persistent is False


def test_worker_routes_execution_to_selected_rq_strategy(monkeypatch):
    calls = []

    def forked(worker, job, queue):
        calls.append(('forked', job, queue))

    def inline(worker, job, queue):
        calls.append(('inline', job, queue))

    monkeypatch.setattr(RQWorker, 'execute_job', forked)
    monkeypatch.setattr(SimpleWorker, 'execute_job', inline)

    worker = worker_with_config(False)
    worker.execute_job('job-1', 'queue')
    worker = worker_with_config(True)
    worker.execute_job('job-2', 'queue')

    assert calls == [
        ('forked', 'job-1', 'queue'),
        ('inline', 'job-2', 'queue'),
    ]


def test_explicit_worker_classes_ignore_automatic_selection():
    config = type('Config', (), {
        'options': {'oorq_persistent_worker': True},
    })()
    non_persistent = NoPersistentWorker.__new__(NoPersistentWorker)
    persistent = PersistentWorker.__new__(PersistentWorker)

    assert non_persistent._persistent_worker_enabled(config) is False
    assert persistent._persistent_worker_enabled(config) is True


def test_persistent_worker_defaults_to_conservative_recycle_limit():
    worker = persistent_worker_state()
    config = type('Config', (), {'options': {}})()

    assert worker._configured_max_jobs(config) == DEFAULT_PERSISTENT_MAX_JOBS


def test_persistent_worker_recycle_limit_can_be_changed_or_disabled():
    worker = persistent_worker_state()

    configured = type('Config', (), {
        'options': {'oorq_persistent_max_jobs': '250'},
    })()
    disabled = type('Config', (), {
        'options': {'oorq_persistent_max_jobs': '0'},
    })()

    assert worker._configured_max_jobs(configured) == 250
    assert worker._configured_max_jobs(disabled) is None


def test_persistent_worker_applies_limit_without_overriding_cli(monkeypatch):
    calls = []
    worker = persistent_worker_state()
    worker.persistent_max_jobs = 100
    monkeypatch.setattr(worker, '_request_erp_shutdown', lambda: None)
    monkeypatch.setattr(
        SimpleWorker, 'work',
        lambda self, *args, **kwargs: calls.append(kwargs['max_jobs']),
    )

    worker.work()
    worker.work(max_jobs=7)

    assert calls == [100, 7]


def test_persistent_worker_stops_erp_services_after_rq_work(monkeypatch):
    calls = []
    worker = persistent_worker_state()
    worker.persistent_max_jobs = 100
    monkeypatch.setattr(
        SimpleWorker, 'work',
        lambda self, *args, **kwargs: calls.append('rq_teardown') or True,
    )
    monkeypatch.setattr(
        worker, '_request_erp_shutdown',
        lambda: calls.append('erp_shutdown'),
    )

    assert worker.work() is True
    assert calls == ['rq_teardown', 'erp_shutdown']


def test_non_persistent_worker_does_not_stop_erp_services_after_rq_work(
        monkeypatch):
    calls = []
    worker = worker_with_config(False)
    worker.persistent_max_jobs = 100
    monkeypatch.setattr(
        RQWorker, 'work',
        lambda self, *args, **kwargs: calls.append('rq') or True,
    )
    monkeypatch.setattr(
        worker, '_request_erp_shutdown', lambda: calls.append('erp_shutdown'),
    )

    assert worker.work() is True
    assert calls == ['rq']


def test_failed_job_marks_persistent_worker_for_orderly_recycle(monkeypatch):
    worker = persistent_worker_state()
    monkeypatch.setattr(SimpleWorker, 'perform_job', lambda *args: False)

    assert worker.perform_job('job', 'queue') is False
    assert worker._stop_requested is True
    assert worker._persistent_exit_reason == 'job_failed'


def test_successful_job_keeps_persistent_worker_available(monkeypatch):
    worker = persistent_worker_state()
    monkeypatch.setattr(SimpleWorker, 'perform_job', lambda *args: True)

    assert worker.perform_job('job', 'queue') is True
    assert worker._stop_requested is False
    assert worker._persistent_exit_reason is None


def test_task_can_request_recycle_after_a_handled_cleanup_risk(monkeypatch):
    worker = persistent_worker_state()
    monkeypatch.setattr(SimpleWorker, 'perform_job', lambda *args: True)
    request_recycle('isolated_execute_failed')

    assert worker.perform_job('job', 'queue') is True
    assert worker._stop_requested is True
    assert worker._persistent_exit_reason == 'isolated_execute_failed'


def test_pubsub_subscriptions_are_database_scoped_and_deduplicated():
    config = type('Config', (), {
        'pubsub_subscriptions': ['database_a.all'],
        '__getitem__': lambda self, key: {'db_name': 'database_a'}[key],
    })()
    queue = type('Queue', (), {'name': 'billing'})()

    assert _pubsub_subscriptions(config, [queue, queue]) == [
        'database_a.all',
        'database_a.worker',
        'database_a.worker.billing',
    ]


def test_worker_bootstraps_once_before_selecting_strategy(monkeypatch):
    calls = []

    class Log(object):
        def addFilter(self, log_filter):
            calls.append('filter')

        def info(self, *args):
            calls.append('strategy-log')

        def warning(self, *args):
            calls.append('warning')

    class Config(object):
        options = {}
        pubsub_subscriptions = []

        def parse(self):
            calls.append('parse')
            self.options['oorq_persistent_worker'] = True

        def __getitem__(self, key):
            return {'db_name': 'test'}[key]

    config = Config()

    def rq_init(worker, *args, **kwargs):
        worker.log = Log()
        worker.queues = []
        worker.job_class = kwargs.get('job_class', worker.job_class)

    monkeypatch.setattr(RQWorker, '__init__', rq_init)
    monkeypatch.setitem(sys.modules, 'netsvc', type('netsvc', (), {
        'SERVICES': {}, 'init_logger': staticmethod(lambda: calls.append('logger')),
    }))
    monkeypatch.setitem(sys.modules, 'tools', type('tools', (), {
        'config': config,
    }))
    monkeypatch.setitem(sys.modules, 'pooler', type('pooler', (), {
        'get_db_and_pool': staticmethod(lambda db: calls.append('pool')),
    }))
    monkeypatch.setitem(sys.modules, 'osv', type('osv_module', (), {
        'osv': type('osv_namespace', (), {
            'osv_pool': staticmethod(lambda: calls.append('osv')),
        }),
    }))
    for module_name in ('workflow', 'report', 'service', 'sql_db'):
        monkeypatch.setitem(sys.modules, module_name, type(module_name, (), {}))

    worker = Worker()

    assert worker.persistent is True
    assert calls.count('parse') == 1
    assert calls.count('filter') == 1
    assert calls.count('logger') == 1
    assert calls.count('strategy-log') == 1
