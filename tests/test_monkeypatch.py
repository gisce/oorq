from tests import OORQTestCase
from oorq.oorq import monkeypatch_sql_db_dsn


class TestMonkeypatch(OORQTestCase):
    def test_monkeypatch_dsn(self):
        import netsvc
        from tools import config
        for attr, value in self.conf.items():
            config[attr] = value
        test_dsn = ''
        for p in ('host', 'port', 'user', 'password'):
            cfg = self.conf.get('db_' + p, '')
            if cfg:
                test_dsn += '%s=%s ' % (p, cfg)
        test_dsn += 'dbname=%s' % self.conf['database']
        import sql_db
        self.assertEqual(sql_db.dsn(self.conf['database']), test_dsn)
        monkeypatch_sql_db_dsn()
        self.assertEqual(sql_db.dsn(self.conf['database']), test_dsn)