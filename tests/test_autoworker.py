from tests import OORQTestCase
import os
from oorq.tasks import execute, isolated_execute
from rq import Queue

class TestAutoworkers(OORQTestCase):
    def test_config(self):
        q = Queue('oorq_tests', async=False)
        job = q.enqueue(execute, self.conf, self.conf['database'], 1, 'res.users', 'read', 1, ['id'])
        self.assertEqual(
            os.environ.get('AUTOWORKER_REDIS_URL'),
            'redis://localhost:6379/0'
        )
