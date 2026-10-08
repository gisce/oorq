import logging
import sys
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
                        closed_databases):
    config = Config(
        root_path='/erp', addons_path='/erp/custom-addons',
        db_maxconn='4', log_level=logging.WARNING,
    )
    tools = module('tools', config=config)
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


def test_execute_restores_process_state_after_business_error(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    caller_context = {'request_id': 'caller'}
    caller_task = object()
    context_stack.push(caller_context)
    task_stack.push(caller_task)
    closed_databases = []

    def fail(*args, **kwargs):
        context_stack.push({'leaked': True})
        task_stack.push(object())
        logging.disable(logging.ERROR)
        logging.getLogger().handlers = []
        raise RuntimeError('business failure')

    install_erp_modules(
        monkeypatch, fail, context_stack, task_stack, closed_databases,
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
    assert closed_databases == ['database']
    assert logging.getLogger().handlers == original_handlers
    assert logging.root.manager.disable == original_disable


def test_two_successful_jobs_do_not_share_stack_state(monkeypatch):
    context_stack = LocalStack()
    task_stack = LocalStack()
    closed_databases = []
    observed_contexts = []

    def succeed(*args, **kwargs):
        observed_contexts.append(dict(context_stack.top))
        context_stack.top['job_only'] = True
        context_stack.push({'leaked': True})
        task_stack.push(object())
        return len(observed_contexts)

    install_erp_modules(
        monkeypatch, succeed, context_stack, task_stack, closed_databases,
    )
    monkeypatch.setattr(tasks.AsyncMode, 'is_async', lambda: True)

    assert tasks.execute({}, 'database', 1, 'model', 'method') == 1
    assert tasks.execute({}, 'database', 1, 'model', 'method') == 2

    assert observed_contexts == [{}, {}]
    assert context_stack.top is None
    assert task_stack.top is None
    assert closed_databases == ['database', 'database']


def test_erp_paths_are_idempotent(monkeypatch):
    original = list(sys.path)
    try:
        monkeypatch.setattr(sys, 'path', list(original))
        tasks._ensure_sys_path('/erp/addons')
        tasks._ensure_sys_path('/erp/addons')
        assert sys.path.count('/erp/addons') == 1
    finally:
        sys.path[:] = original
