from __future__ import absolute_import, unicode_literals
import time
from .test_oorq import TestOORQ
from destral.transaction import Transaction
from service.taskmanager import TaskManager
from tools.service_utils import WebserviceBase
from ctx import current_session


class TestTaskManager(TestOORQ):
    def setUp(self):
        super(TestTaskManager, self).setUp()


    def test_task_is_propagated_into_the_job_if_is_deferred(self):
        """
        Test that the task is propagated into the job if it is deferred.
        """
        pool = self.openerp.pool
        partner_obj = pool.get('res.partner')
        task_obj = pool.get('ir.task')
        with Transaction().start(self.database) as txn:
            with WebserviceBase(uid=txn.user):
                self.assertEqual(current_session.uid, txn.user)
                cursor = txn.cursor
                uid = txn.user
                ids = [self.new_partner_ids[0]]
                values = {'name': 'Deferred Task Test'}
                context = txn.context
                with TaskManager("Test Task") as t:
                    t.defer()
                    partner_obj.write_async_with_task(cursor, uid, ids, values, context)
            cursor.commit()
            task_id = t.id
            time.sleep(2)
            self._empty_wait()
            time.sleep(1)
            task_data = task_obj.browse(cursor, uid, task_id)
            self.assertEqual(task_data.state, 'running')
            self.assertEqual(task_data.progress, 10)

