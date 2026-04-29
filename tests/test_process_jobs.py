# coding=utf-8
import imp
import os
import sys
import types
import unittest


class FakeLogger(object):
    def notifyChannel(self, *args, **kwargs):
        pass


class FakeSignal(object):
    def connect(self, callback):
        pass


class FakeTaskManager(object):
    @staticmethod
    def current_task():
        return None


class FakeJob(object):
    deleted = []
    fetched = []

    def __init__(self, job_id):
        self.id = job_id

    @classmethod
    def fetch(cls, job_id, connection=None):
        cls.fetched.append((job_id, connection))
        return cls(job_id)

    def delete(self):
        self.deleted.append(self.id)


class FakeQueue(object):
    enqueued = []

    def __init__(self, name, connection=None, **kwargs):
        self.name = name
        self.connection = connection

    def enqueue_job(self, job, at_front=False):
        self.enqueued.append((self.name, job.id, at_front, self.connection))


def install_import_stubs():
    oorq_package = types.ModuleType('oorq')
    oorq_package.__path__ = []
    sys.modules['oorq'] = oorq_package

    oorq_module = types.ModuleType('oorq.oorq')
    oorq_module.setup_redis_connection = lambda: 'redis-conn'
    oorq_module.set_hash_job = lambda job: None
    oorq_module.get_redis_url = lambda conn: 'redis://localhost:6379/0'

    class FakeAsyncMode(object):
        @staticmethod
        def is_async():
            return True

    oorq_module.AsyncMode = FakeAsyncMode
    sys.modules['oorq.oorq'] = oorq_module

    exceptions = types.ModuleType('oorq.exceptions')
    sys.modules['oorq.exceptions'] = exceptions

    tasks = types.ModuleType('oorq.tasks')
    tasks.make_chunks = lambda ids, n_chunks=None, size=None: [ids]
    tasks.execute = lambda *args, **kwargs: None
    tasks.isolated_execute = lambda *args, **kwargs: None
    tasks.update_jobs_group = lambda *args, **kwargs: None
    sys.modules['oorq.tasks'] = tasks

    rq_module = types.ModuleType('rq')
    rq_module.Queue = FakeQueue
    rq_module.get_current_job = lambda: None
    sys.modules['rq'] = rq_module

    rq_job_module = types.ModuleType('rq.job')
    rq_job_module.Job = FakeJob
    sys.modules['rq.job'] = rq_job_module

    rq_exceptions_module = types.ModuleType('rq.exceptions')

    class FakeNoSuchJobError(Exception):
        pass

    rq_exceptions_module.NoSuchJobError = FakeNoSuchJobError
    sys.modules['rq.exceptions'] = rq_exceptions_module

    osconf_module = types.ModuleType('osconf')
    osconf_module.config_from_environment = lambda prefix, **kwargs: kwargs
    sys.modules['osconf'] = osconf_module

    if 'tools' not in sys.modules:
        tools = types.ModuleType('tools')
        tools.config = {'database': 'test'}
        sys.modules['tools'] = tools
    if 'netsvc' not in sys.modules:
        netsvc = types.ModuleType('netsvc')
        netsvc.LOG_INFO = 20
        netsvc.LOG_WARNING = 30
        netsvc.SERVICES = {}
        netsvc.Logger = FakeLogger
        sys.modules['netsvc'] = netsvc
    if 'signals' not in sys.modules:
        signals = types.ModuleType('signals')
        signals.DB_CURSOR_COMMIT = FakeSignal()
        signals.DB_CURSOR_ROLLBACK = FakeSignal()
        signals.DB_CURSOR_ROLLBACK_SAVEPOINT = FakeSignal()
        signals.DB_CURSOR_SAVEPOINT = FakeSignal()
        sys.modules['signals'] = signals
    if 'autoworker' not in sys.modules:
        autoworker = types.ModuleType('autoworker')
        autoworker.AutoWorker = object
        sys.modules['autoworker'] = autoworker
    if 'ctx' not in sys.modules:
        ctx = types.ModuleType('ctx')
        ctx.sudo = None
        sys.modules['ctx'] = ctx
    if 'service' not in sys.modules:
        service = types.ModuleType('service')
        sys.modules['service'] = service
    if 'service.taskmanager' not in sys.modules:
        taskmanager = types.ModuleType('service.taskmanager')
        taskmanager.TaskManager = FakeTaskManager
        sys.modules['service.taskmanager'] = taskmanager


def load_decorators_module():
    install_import_stubs()
    decorators_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'oorq', 'decorators.py'
    )
    if 'oorq.decorators' in sys.modules:
        del sys.modules['oorq.decorators']
    return imp.load_source('oorq.decorators', decorators_path)


class FakeCursor(object):
    pass


class TestProcessJobs(unittest.TestCase):
    def setUp(self):
        self.decorators = load_decorators_module()
        self.decorators.ProcessJobs.JOBS_TO_PROCESS = {}
        FakeJob.deleted = []
        FakeJob.fetched = []
        FakeQueue.enqueued = []
        self.decorators.Job = FakeJob
        self.decorators.Queue = FakeQueue
        self.decorators.setup_redis_connection = lambda: 'redis-conn'

    def test_add_job_keeps_minimal_reference_only(self):
        cursor = FakeCursor()
        job = FakeJob('job-1')
        queue = FakeQueue('queue-1', connection='original-conn')

        self.decorators.ProcessJobs.add_job(id(cursor), job, queue, True)

        pending = self.decorators.ProcessJobs.JOBS_TO_PROCESS[id(cursor)]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].job_id, 'job-1')
        self.assertEqual(pending[0].queue_name, 'queue-1')
        self.assertTrue(pending[0].at_front)
        self.assertFalse(hasattr(pending[0], 'job'))
        self.assertFalse(hasattr(pending[0], 'queue'))

    def test_commit_fetches_job_and_queue_from_redis(self):
        cursor = FakeCursor()
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-1'), FakeQueue('queue-1'), False
        )

        self.decorators.ProcessJobs.commit(cursor)

        self.assertEqual(FakeJob.fetched, [('job-1', 'redis-conn')])
        self.assertEqual(FakeQueue.enqueued, [('queue-1', 'job-1', False, 'redis-conn')])
        self.assertNotIn(id(cursor), self.decorators.ProcessJobs.JOBS_TO_PROCESS)

    def test_rollback_deletes_pending_jobs_from_redis(self):
        cursor = FakeCursor()
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-1'), FakeQueue('queue-1'), False
        )
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-2'), FakeQueue('queue-1'), True
        )

        self.decorators.ProcessJobs.rollback(cursor)

        self.assertEqual(FakeJob.deleted, ['job-1', 'job-2'])
        self.assertEqual(FakeQueue.enqueued, [])
        self.assertNotIn(id(cursor), self.decorators.ProcessJobs.JOBS_TO_PROCESS)

    def test_rollback_savepoint_deletes_discarded_jobs_only(self):
        cursor = FakeCursor()
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-before'), FakeQueue('queue-1'), False
        )
        self.decorators.ProcessJobs.savepoint(cursor, 'sp1')
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-after'), FakeQueue('queue-1'), False
        )

        self.decorators.ProcessJobs.rollback_savepoint(cursor, 'sp1')

        self.assertEqual(FakeJob.deleted, ['job-after'])
        pending = self.decorators.ProcessJobs.JOBS_TO_PROCESS[id(cursor)]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].job_id, 'job-before')


if __name__ == '__main__':
    unittest.main()
