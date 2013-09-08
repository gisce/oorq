from tests import OORQTestCase
from oorq.tasks import execute
from rq import Queue, Worker

class TestAPI(OORQTestCase):
    def test_enqueue_job(self):
        q = Queue('oorq_tests', async=False)
        conf = {
            'database': 'oerp6',
            'db_user': 'eduard',
            'db_host': 'localhost',
            'pg_path': '/Users/eduard/Projects/virtualenvs/gisce-erp/bin/psql',
            'addons_path': '/Users/eduard/Projectes/oerp6/server/bin/addons'
        }
        job = q.enqueue(execute, conf, conf['database'], 1, 'res.users', 'search', [('id', '=', 1)])
        self.assertEqual(job.result, [1])