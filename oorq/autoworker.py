# coding=utf-8
from __future__ import absolute_import
from autoworker import AutoWorker as AutoWorkerBase
from oorq.decorators import log
from signals import DB_CURSOR_COMMIT, DB_CURSOR_ROLLBACK


class AutoWorkerRegister(object):

    AUTOWORKERS_TO_PROCESS = {}

    @classmethod
    def add_autoworker(cls, transaction_id, worker):
        cls.AUTOWORKERS_TO_PROCESS.setdefault(transaction_id, [])
        cls.AUTOWORKERS_TO_PROCESS[transaction_id].append(
            worker
        )

    @staticmethod
    def commit(cursor):
        transaction_id = id(cursor)
        workers = AutoWorkerRegister.AUTOWORKERS_TO_PROCESS.pop(
            transaction_id, []
        )
        for worker in workers:
            log('Spawing worker for queue {} from commit transaction {}'.format(
                worker.queue.name, transaction_id
            ))
            worker.work()

    @staticmethod
    def rollback(cursor):
        transaction_id = id(cursor)
        workers = AutoWorkerRegister.AUTOWORKERS_TO_PROCESS.pop(
            transaction_id, []
        )
        if workers:
            log('Cancelling {} workers from rollback of transaction {}'.format(
                len(workers), transaction_id
            ))


DB_CURSOR_COMMIT.connect(AutoWorkerRegister.commit)
DB_CURSOR_ROLLBACK.connect(AutoWorkerRegister.rollback)


class AutoWorker(AutoWorkerBase):

    def work(self, cursor=None):
        if cursor is None:
            super(AutoWorker, self).work()
        else:
            transaction_id = id(cursor)
            AutoWorkerRegister.add_autoworker(transaction_id, self)
