# -*- coding: utf-8 -*-
from destral import testing
from destral.transaction import Transaction
import os

from autoworker import AutoWorker

class TestAutoworker(testing.OOTestCase):
    ''' Test Autoworker '''
    def test_autoworker_redis_connection(self):
        partner_obj = self.openerp.pool.get('res.partner')
        with Transaction().start(self.database) as txn:
            cursor = txn.cursor
            uid = txn.user

            partner_obj.write_not_async(cursor, uid, [1], {'name': "Test Partner"})
            assert os.environ.get('AUTOWORKER_REDIS_URL', False) != False