from tests import OORQTestCase
from oorq.tasks import execute, isolated_execute
from rq import Queue, Worker

class TestAPI(OORQTestCase):
    def test_enqueue_job_search(self):
        q = Queue('oorq_tests', async=False)
        job = q.enqueue(execute, self.conf, self.conf['database'], 1, 'res.users', 'search', [('id', '=', 1)])
        self.assertEqual(job.result, [1])

    def test_enqueue_job_write(self):
        q = Queue('oorq_tests', async=False)
        job = q.enqueue(execute, self.conf, self.conf['database'], 1, 'res.users', 'write', [1], {'name': 'User'})
        self.assertEqual(job.result, True)

    def test_enqueue_job_read(self):
        q = Queue('oorq_tests', async=False)
        job = q.enqueue(execute, self.conf, self.conf['database'], 1, 'res.users', 'read', 1, ['id'])
        self.assertDictContainsSubset({'id': 1}, job.result)

    def test_isolated_job(self):
        q = Queue('oorq_tests', async=False)
        job = q.enqueue(execute, self.conf, self.conf['database'], 1, 'res.users', 'search', [])
        ids = job.result
        job = q.enqueue(
            isolated_execute,
            self.conf,
            self.conf['database'],
            1,
            'res.users',
            'write',
            ids,
            {'name': 'User'})
        self.assertEqual(job.result, [True] * len(ids))
