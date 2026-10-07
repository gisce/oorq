from rq import Worker as RQWorker
from rq.worker import SimpleWorker

from oorq.worker import ERPWorkerMixin, PersistentWorker, Worker, WorkerJob


def function(method):
    return getattr(method, 'im_func', method)


def test_default_worker_keeps_fork_based_rq_worker():
    assert issubclass(Worker, ERPWorkerMixin)
    assert issubclass(Worker, RQWorker)
    assert function(Worker.execute_job) is function(RQWorker.execute_job)
    assert Worker.job_class is WorkerJob


def test_persistent_worker_uses_simple_worker_execution():
    assert issubclass(PersistentWorker, ERPWorkerMixin)
    assert issubclass(PersistentWorker, SimpleWorker)
    assert function(PersistentWorker.execute_job) is function(SimpleWorker.execute_job)
    assert PersistentWorker.job_class is WorkerJob
