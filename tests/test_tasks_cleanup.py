import logging
import sys
from threading import current_thread
from types import ModuleType

import pytest
from werkzeug.local import LocalStack

from oorq import tasks


class ContextManager(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def set_tag(self, key, value):
        pass


class Config(dict):
    pass


def module(name, **attributes):
    value = ModuleType(name)
    for key, attribute in attributes.items():
        setattr(value, key, attribute)
    return value


def install_erp_modules(monkeypatch, execute, context_stack, task_stack,
                        closed_databases, cleaned_databases=None):
    if cleaned_databases is None:
        cleaned_databases = []
    config = Config(
        root_path='/erp', addons_path='/erp/custom-addons',
        db_maxconn='4', log_level=logging.WARNING,
    )
    tools = module(
        'tools', config=config,
        cache=type('Cache', (object,), {
            'clean_caches_for_db': staticmethod(cleaned_databases.append),
        }),
    )
    pool = type('Pool', (object,), {'_ready': True})()
    pooler = module(
        'pooler',
        get_db_and_pool=lambda dbname: (object(), pool),
    )
    osv_pool = type('OSVPool', (object,), {'execute': execute})()
    osv_namespace = type(
        'OSVNamespace', (object,), {'osv_pool': lambda self: osv_pool}
    )()
    sql_db = module(
        'sql_db', _Pool=object(),
        ConnectionPool=lambda size: object(),
        close_db=lambda dbname: closed_databases.append(dbname),
    )
    security = module(
        'service.security',
        Sudo=lambda **kwargs: ContextManager(),
    )
    taskmanager = module(
        'service.taskmanager',
        Task=lambda task_id: {'id': task_id},
        TASK_CONTEXT_STACK=task_stack,
    )
    service_utils = module(
        'tools.service_utils',
        WebServiceTracker=lambda **kwargs: ContextManager(),
        SimpleGlobalUUIDGenerator=lambda: ContextManager(),
    )
    sentry_sdk = module(
        'sentry_sdk',
        configure_scope=lambda: ContextManager(),
        capture_exception=lambda exception: None,
    )
    modules = {
        'netsvc': module('netsvc'),
        'tools': tools,
        'tools.service_utils': service_utils,
        'pooler': pooler,
        'osv': module('osv', osv=osv_namespace),
        'workflow': module('workflow'),
        'report': module('report'),
        'service': module('service'),
        'service.security': security,
        'service.taskmanager': taskmanager,
        'sql_db': sql_db,
        'ctx': module('ctx', _context_stack=context_stack),
        'sentry_sdk': sentry_sdk,
    }
    for name, value in modules.items():
        monkeypatch.setitem(sys.modules, name, value)


def test_stack_frame_preserves_caller_and_removes_leaked_frames():
    stack = LocalStack()
    caller = object()
    frame = object()
    stack.push(caller)

    with tasks._stack_frame(stack, frame):
        stack.push(object())

    assert stack.top is caller


def test_stack_frame_restores_duplicate_object_frames():
    stack = LocalStack()
    caller = object()
    frame = object()
    stack.push(caller)

    with tasks._stack_frame(stack, frame):
        stack.push(frame)

    assert stack.pop() is caller
    assert stack.top is None


def test_preserve_stack_restores_duplicate_previous_frame():
    stack = LocalStack()
    previous = object()
    stack.push(previous)

    with tasks._preserve_stack(stack):
        stack.push(previous)

    assert stack.pop() is previous
    assert stack.top is None


def test_preserve_stack_restores_frames_removed_by_business_code():
    stack = LocalStack()
    caller = object()
    stack.push(caller)

    with tasks._preserve_stack(stack):
        assert stack.pop() is caller

    assert stack.pop() is caller
    assert stack.top is None


def test_execute_restores_process_state_after_business_error(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    caller_context = {'request_id': 'caller'}
    caller_task = object()
    context_stack.push(caller_context)
    task_stack.push(caller_task)
    closed_databases = []
    cleaned_databases = []
    thread = current_thread()
    monkeypatch.setattr(thread, 'dbname', 'caller_database', raising=False)

    def fail(*args, **kwargs):
        context_stack.push({'leaked': True})
        task_stack.push(object())
        logging.disable(logging.ERROR)
        logging.getLogger().handlers = []
        raise RuntimeError('business failure')

    install_erp_modules(
        monkeypatch, fail, context_stack, task_stack, closed_databases,
        cleaned_databases,
    )
    monkeypatch.setattr(tasks.AsyncMode, 'is_async', lambda: True)
    original_handlers = list(logging.getLogger().handlers)
    original_disable = logging.root.manager.disable

    with pytest.raises(RuntimeError, match='business failure'):
        tasks.execute(
            {}, 'database', 1, 'model', 'method',
            current_task_id=42,
        )

    assert context_stack.top is caller_context
    assert task_stack.top is caller_task
    assert closed_databases == []
    assert cleaned_databases == ['database']
    assert thread.dbname == 'caller_database'
    assert logging.getLogger().handlers == original_handlers
    assert logging.root.manager.disable == original_disable


def test_two_successful_jobs_do_not_share_stack_state(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    closed_databases = []
    cleaned_databases = []
    observed_contexts = []
    pools = []

    def succeed(*args, **kwargs):
        pools.append(sys.modules['sql_db']._Pool)
        current_thread().dbname = 'database'
        observed_contexts.append(dict(context_stack.top))
        context_stack.top['job_only'] = True
        context_stack.push(context_stack.top)
        if task_stack.top is not None:
            task_stack.push(task_stack.top)
        return len(observed_contexts)

    install_erp_modules(
        monkeypatch, succeed, context_stack, task_stack, closed_databases,
        cleaned_databases,
    )
    monkeypatch.setattr(tasks.AsyncMode, 'is_async', lambda: True)

    assert tasks.execute(
        {}, 'database', 1, 'model', 'method', current_task_id=1,
    ) == 1
    assert tasks.execute(
        {}, 'database', 1, 'model', 'method', current_task_id=2,
    ) == 2

    assert observed_contexts == [{}, {}]
    assert context_stack.top is None
    assert task_stack.top is None
    assert closed_databases == []
    assert cleaned_databases == []
    assert pools[0] is pools[1]
    assert not hasattr(current_thread(), 'dbname')


def test_report_between_execute_jobs_preserves_pool_and_caches(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    closed_databases = []
    cleaned_databases = []
    pools = []

    def succeed(*args, **kwargs):
        pools.append(sys.modules['sql_db']._Pool)
        return len(pools)

    install_erp_modules(
        monkeypatch, succeed, context_stack, task_stack, closed_databases,
        cleaned_databases,
    )

    class Cursor(object):
        def close(self):
            pass

    class Connection(object):
        def cursor(self, **kwargs):
            return Cursor()

    class ReportService(object):
        _service = type('Service', (object,), {'model': 'model'})()

        def create(self, cursor, uid, ids, datas, context):
            pools.append(sys.modules['sql_db']._Pool)
            return b'report', 'pdf'

    class Job(object):
        meta = {}

        def save(self):
            pass

    sys.modules['sql_db'].db_connect = lambda dbname: Connection()
    sys.modules['netsvc'].LocalService = lambda name: ReportService()
    monkeypatch.setattr(tasks, 'get_current_job', lambda: Job())
    monkeypatch.setattr(tasks.AsyncMode, 'is_async', lambda: True)

    assert tasks.execute({}, 'database', 1, 'model', 'method') == 1
    assert tasks.report(
        {}, 'database', 1, 'sample', [1], datas={}, context={},
    ) == (b'report', 'pdf')
    assert tasks.execute({}, 'database', 1, 'model', 'method') == 3

    assert closed_databases == []
    assert cleaned_databases == []
    assert pools[0] is pools[1] is pools[2]


def test_update_jobs_group_preserves_pool_and_caches(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    closed_databases = []
    cleaned_databases = []
    install_erp_modules(
        monkeypatch, lambda *args, **kwargs: None,
        context_stack, task_stack, closed_databases, cleaned_databases,
    )

    class JobsPool(object):
        def __init__(self, *args):
            pass

        def add_job(self, job):
            pass

        def join(self):
            pass

    monkeypatch.setattr(tasks, 'StoredJobsPool', JobsPool)
    monkeypatch.setattr(tasks, 'setup_redis_connection', lambda: object())
    monkeypatch.setattr(
        tasks.Job, 'fetch', staticmethod(lambda job_id: object()),
    )

    tasks.update_jobs_group({}, 'database', 1, 'group', False, ['job'])

    assert closed_databases == []
    assert cleaned_databases == []


def test_erp_paths_are_idempotent(monkeypatch):
    original = list(sys.path)
    try:
        monkeypatch.setattr(sys, 'path', list(original))
        tasks._ensure_sys_path('/erp/addons')
        tasks._ensure_sys_path('/erp/addons')
        assert sys.path.count('/erp/addons') == 1
    finally:
        sys.path[:] = original


def test_isolated_execute_stops_after_job_timeout(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    executed_ids = []

    def timeout_first_id(*args, **kwargs):
        executed_ids.extend(args[-1])
        raise tasks.JobTimeoutException('deadline')

    install_erp_modules(
        monkeypatch, timeout_first_id, context_stack, task_stack, [],
    )

    with pytest.raises(tasks.JobTimeoutException, match='deadline'):
        tasks.isolated_execute(
            {}, 'database', 1, 'model', 'method', [1, 2],
        )

    assert executed_ids == [1]


def test_other_entrypoints_restore_process_state(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    caller_context = object()
    caller_task = object()
    context_stack.push(caller_context)
    task_stack.push(caller_task)
    install_erp_modules(
        monkeypatch, lambda *args, **kwargs: None,
        context_stack, task_stack, [],
    )
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_disable = logging.root.manager.disable

    @tasks._restore_entrypoint_state
    def leaking_entrypoint(conf_attrs, dbname):
        context_stack.push(object())
        task_stack.push(object())
        root_logger.handlers = []
        logging.disable(logging.ERROR)
        current_thread().dbname = dbname
        raise RuntimeError('entrypoint failure')

    with pytest.raises(RuntimeError, match='entrypoint failure'):
        leaking_entrypoint({}, 'database')

    assert context_stack.pop() is caller_context
    assert task_stack.pop() is caller_task
    assert root_logger.handlers == original_handlers
    assert logging.root.manager.disable == original_disable
    assert not hasattr(current_thread(), 'dbname')
