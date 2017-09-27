from tests import OORQTestCase
import os


class TestAutoworkers(OORQTestCase):
    def test_config(self):
        self.assertEqual(
            os.environ.get('AUTOWORKER_REDIS_URL'),
            'redis://localhost:6379/0'
        )
