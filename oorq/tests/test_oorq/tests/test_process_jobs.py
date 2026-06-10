# coding=utf-8
import unittest
from oorq import decorators


class FakeNoSuchJobError(Exception):
    pass


class FakeJob(object):
    deleted = []
    fetched = []
    missing_jobs = set()

    def __init__(self, job_id):
        self.id = job_id

    @classmethod
    def fetch(cls, job_id, connection=None):
        cls.fetched.append((job_id, connection))
        if job_id in cls.missing_jobs:
            raise FakeNoSuchJobError()
        return cls(job_id)

    def delete(self):
        self.deleted.append(self.id)


class FakeQueue(object):
    enqueued = []
    fail_on_job_ids = set()

    def __init__(self, name, connection=None, **kwargs):
        self.name = name
        self.connection = connection

    def enqueue_job(self, job, at_front=False):
        if job.id in self.fail_on_job_ids:
            raise RuntimeError('enqueue failed')
        self.enqueued.append((self.name, job.id, at_front, self.connection))


class FakeCursor(object):
    pass


_MISSING = object()


class TestProcessJobs(unittest.TestCase):
    def setUp(self):
        self.decorators = decorators
        self.original_job = self.decorators.Job
        self.original_queue = self.decorators.Queue
        self.original_no_such_job_error = getattr(
            self.decorators, 'NoSuchJobError', _MISSING
        )
        self.original_setup_redis_connection = (
            self.decorators.setup_redis_connection
        )
        self.decorators.ProcessJobs.JOBS_TO_PROCESS = {}
        FakeJob.deleted = []
        FakeJob.fetched = []
        FakeJob.missing_jobs = set()
        FakeQueue.enqueued = []
        FakeQueue.fail_on_job_ids = set()
        self.decorators.Job = FakeJob
        self.decorators.Queue = FakeQueue
        self.decorators.NoSuchJobError = FakeNoSuchJobError
        self.decorators.setup_redis_connection = lambda: 'redis-conn'

    def tearDown(self):
        self.decorators.Job = self.original_job
        self.decorators.Queue = self.original_queue
        if self.original_no_such_job_error is _MISSING:
            try:
                del self.decorators.NoSuchJobError
            except AttributeError:
                pass
        else:
            self.decorators.NoSuchJobError = self.original_no_such_job_error
        self.decorators.setup_redis_connection = (
            self.original_setup_redis_connection
        )
        self.decorators.ProcessJobs.JOBS_TO_PROCESS = {}

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

    def test_commit_skips_missing_jobs(self):
        cursor = FakeCursor()
        FakeJob.missing_jobs = set(['job-missing'])
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-missing'), FakeQueue('queue-1'), False
        )
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-ok'), FakeQueue('queue-1'), False
        )

        self.decorators.ProcessJobs.commit(cursor)

        self.assertEqual(FakeQueue.enqueued, [('queue-1', 'job-ok', False, 'redis-conn')])
        self.assertNotIn(id(cursor), self.decorators.ProcessJobs.JOBS_TO_PROCESS)

    def test_commit_keeps_pending_jobs_if_enqueue_fails(self):
        cursor = FakeCursor()
        FakeQueue.fail_on_job_ids = set(['job-2'])
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-1'), FakeQueue('queue-1'), False
        )
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-2'), FakeQueue('queue-1'), False
        )
        self.decorators.ProcessJobs.add_job(
            id(cursor), FakeJob('job-3'), FakeQueue('queue-1'), False
        )

        with self.assertRaises(RuntimeError):
            self.decorators.ProcessJobs.commit(cursor)

        self.assertEqual(FakeQueue.enqueued, [('queue-1', 'job-1', False, 'redis-conn')])
        pending = self.decorators.ProcessJobs.JOBS_TO_PROCESS[id(cursor)]
        self.assertEqual([job.job_id for job in pending], ['job-2', 'job-3'])

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
