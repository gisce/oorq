# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from destral import testing
from destral.transaction import Transaction
import subprocess
from copy import copy
from datetime import datetime
from rq import Worker
from time import sleep
import os
from osv import osv



class TestOORQ(testing.OOTestCase):
    _queues = ['default', 'dependency']

    def setUp(self):
        super(TestOORQ, self).setUp()
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj = self.openerp.pool.get('res.partner')
            self.partner_ids = partner_obj.search(cursor, uid, [], context={'active_test': False})
            self.new_partner_ids = [
                partner_obj.create(cursor, uid, {'name': 'Gumersindo', 'active': 0}),
                partner_obj.create(cursor, uid, {'name': 'Sandro Rey', 'active': 0}),
                partner_obj.create(cursor, uid, {'name': 'Pedro', 'active': 0}),
                partner_obj.create(cursor, uid, {'name': 'Manolo', 'active': 0}),
                partner_obj.create(cursor, uid, {'name': 'Jaime', 'active': 0}),
                partner_obj.create(cursor, uid, {'name': 'Carlos', 'active': 0}),
            ]
            cursor.commit()

    def tearDown(self):
        super(TestOORQ, self).tearDown()
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj = self.openerp.pool.get('res.partner')
            to_unlink = partner_obj.search(
                cursor, uid, [('id', 'not in', self.partner_ids)], context={'active_test': False}
            )
            if to_unlink:
                partner_obj.unlink(cursor, uid, to_unlink)

    def _empty_wait(self, timeout=20):
        from redis_pool import setup_redis_connection
        redis_conn = setup_redis_connection()
        start_time = datetime.now()
        workers = Worker.all(connection=redis_conn)
        while True:
            if (datetime.now() - start_time).seconds >= timeout:
                raise Exception("Timeout waiting for jobs to finish")

            all_empty = True
            for queue_name in self._queues:
                queue_size = redis_conn.llen(queue_name)
                if queue_size > 0:
                    all_empty = False
                    break

            if all_empty:
                active_workers = any(worker.get_state() == 'busy' for worker in workers)
                if not active_workers:
                    break

            sleep(1)

    @classmethod
    def setUpClass(cls):
        super(TestOORQ, cls).setUpClass()
        from tools import config
        cls.ori_oorq_async = os.environ.get('OORQ_ASYNC', 'True')
        os.environ['OORQ_ASYNC'] = 'True'
        cls.log_file = open('/tmp/worker.log', 'w')
        cls.env = os.environ.copy()
        cls.env['OPENERP_IGNORE_PUBSUB'] = "1"
        cls.workers_process = []
        cls.redis_url = config.get('redis_url') or 'redis://localhost:6379/0'
        cls.cmd = ['rq', 'worker', '-u', cls.redis_url, '-w', 'oorq.worker.Worker']
        cls.cwd = os.getcwd()
        for queue_name in cls._queues:
            _cmd = copy(cls.cmd)
            _cmd.extend([queue_name])
            cls.workers_process.append(
                subprocess.Popen(_cmd, env=cls.env, cwd=cls.cwd, stdout=cls.log_file, stderr=subprocess.STDOUT)
            )

    @classmethod
    def tearDownClass(cls):
        for _proc in cls.workers_process:
            _proc.kill()
        cls.log_file.close()
        with open('/tmp/worker.log', 'r') as f:
            print(f.read())
        os.environ['OORQ_ASYNC'] = cls.ori_oorq_async
        super(TestOORQ, cls).tearDownClass()

    def test_write_async_on_commit(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj.test_write_async(cursor, uid, self.new_partner_ids, {'active': True})
            cursor.commit()
            # This sleep is needed because the enqueue delay
            sleep(2)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(r['active'] for r in res))

    def test_write_async_on_commit_rollback_no_enqueue_on_error(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user

            def simulate_ws():
                try:
                    partner_obj.test_no_enqueue_on_rollback(cursor, uid, self.new_partner_ids, {'active': True})
                    cursor.commit()
                except Exception as e:
                    cursor.rollback()
                    raise e
            with self.assertRaises(osv.except_osv):
                simulate_ws()
            # This sleep is needed because the enqueue delay
            sleep(2)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(not r['active'] for r in res))

    def test_write_async_split_isolated(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj.test_write_split(cursor, uid, self.new_partner_ids, {'active': True})
            # This sleep is needed because the enqueue delay
        sleep(2)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(r['active'] for r in res))

    def test_write_async_split_size(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj.test_write_split_size(cursor, uid, self.new_partner_ids, {'active': True})
            # This sleep is needed because the enqueue delay
        sleep(2)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(r['active'] for r in res))

    def test_dependency_job(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj.test_dependency_job(cursor, uid, self.new_partner_ids, {'active': True})
            # This sleep is needed because the enqueue delay 10+2
            sleep(12)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(r['active'] for r in res))

    def test_dependency_job_on_commit(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            partner_obj.dependency_job_on_commit(cursor, uid, self.new_partner_ids, {'active': True})
            cursor.commit()
            # This sleep is needed because the enqueue delay 5+2
            sleep(7)
        self._empty_wait()
        sleep(1)
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user
            res = partner_obj.read(cursor, uid, self.new_partner_ids, ['active'])
        self.assertEqual(len(res), 6)
        self.assertTrue(all(r['active'] for r in res))
