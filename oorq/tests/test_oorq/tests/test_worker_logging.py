from __future__ import absolute_import

import logging
import sys
import types
import unittest

from mock import MagicMock, Mock, PropertyMock, patch
from redis import Redis
from rq import Queue, Worker as RQWorker
from rq.job import Job
from six import text_type

from oorq.worker import Worker, WorkerJob


class TestWorkerLogging(unittest.TestCase):
    def setUp(self):
        self.connection = Redis()
        self.config = {
            'minio_endpoint': 'private-endpoint',
            'minio_secret_key': 'private-secret',
            'nested': {'password': 'nested-secret'},
        }
        self.args = (
            self.config, 'ecasa_comer', 43, 'giscedata.lectures.comptador',
            'get_lectures_from_pool_async', [147361], '2026-09-30', 'job-token',
        )
        self.kwargs = {'context': {'active_ids': [146]},
                       'sudo': {'gid': 132, 'uid': 43}}

    def redis_data(self, job):
        return dict(
            (key, value.encode('utf-8') if isinstance(value, text_type) else value)
            for key, value in job.to_dict().items()
        )

    def restore_job(self, job):
        restored = WorkerJob(id=job.id, connection=self.connection)
        restored.restore(self.redis_data(job))
        return restored

    def test_existing_jobs_omit_configuration_from_description(self):
        for task in ('execute', 'isolated_execute', 'report',
                     'update_jobs_group'):
            func = 'oorq.tasks.' + task
            job = Job.create(func, args=self.args, kwargs=self.kwargs,
                             connection=self.connection)
            restored = self.restore_job(job)
            expected = Job.create(func, args=self.args[1:], kwargs=self.kwargs,
                                  connection=self.connection)

            self.assertEqual(restored.description, expected.description)
            self.assertEqual(restored.args, job.args)
            self.assertEqual(restored.kwargs, job.kwargs)
            self.assertEqual(restored.data, job.data)
            self.assertEqual(restored.get_call_string(), job.get_call_string())
            for key in self.config:
                self.assertNotIn(key, restored.description)

    def test_configuration_passed_by_keyword_is_omitted(self):
        kwargs = dict(self.kwargs, conf_attrs=self.config, dbname='ecasa_comer')
        job = Job.create('oorq.tasks.execute', kwargs=kwargs,
                         connection=self.connection)
        restored = self.restore_job(job)

        self.assertNotIn('conf_attrs', restored.description)
        self.assertNotIn('private-secret', restored.description)
        self.assertIn("dbname='ecasa_comer'", restored.description)
        self.assertEqual(restored.kwargs, kwargs)

    def test_other_jobs_keep_their_description_and_arguments(self):
        job = Job.create('other.tasks.execute', args=(self.config,),
                         description='Custom description',
                         connection=self.connection)
        restored = self.restore_job(job)

        self.assertEqual(restored.description, 'Custom description')
        self.assertEqual(restored.args, job.args)

    def test_invalid_payload_does_not_expose_saved_description(self):
        job = Job.create('oorq.tasks.execute', args=self.args,
                         connection=self.connection)
        data = self.redis_data(job)
        data['data'] = b'invalid pickle'
        restored = WorkerJob(connection=self.connection)
        restored.restore(data)

        self.assertEqual(restored.description, '<DeserializationError>')

    def test_worker_uses_safe_jobs_with_rq_cli_defaults(self):
        modules = dict((name, types.ModuleType(name)) for name in (
            'netsvc', 'tools', 'pooler', 'osv', 'workflow', 'report',
            'service', 'sql_db',
        ))
        modules['netsvc'].SERVICES = {}
        modules['netsvc'].init_logger = Mock()
        modules['tools'].config = MagicMock()
        modules['pooler'].get_db_and_pool = Mock()
        modules['osv'].osv = Mock()
        # Skip optional pubsub/Sentry initialization in the ERP test process too.
        modules['service.pubsub'] = None
        logger = logging.Logger('test.worker.init')
        with patch('rq.worker.logger', logger):
            with patch.dict(sys.modules, modules):
                with patch.object(sys, 'argv', sys.argv[:]):
                    worker = Worker(['create_reads'], connection=self.connection,
                                    job_class=Job, prepare_for_work=False)

        self.assertIs(worker.job_class, WorkerJob)
        self.assertIn(Worker.log_filter, worker.log.filters)

    def test_dequeue_log_keeps_task_details_without_configuration(self):
        job = Job.create('oorq.tasks.execute', args=self.args,
                         kwargs=self.kwargs, connection=self.connection)
        queue = Queue('create_reads', connection=self.connection)
        worker = Worker.__new__(Worker)
        RQWorker.__init__(worker, [queue], connection=self.connection,
                          prepare_for_work=False)
        worker.heartbeat = Mock()
        worker.set_state = Mock()
        worker.procline = Mock()
        worker.get_redis_server_version = Mock(return_value=(7, 0, 0))
        worker.log = logging.Logger('test.worker')
        handler = logging.Handler()
        handler.emit = Mock()
        worker.log.addHandler(handler)

        def dequeue(*args, **kwargs):
            restored = kwargs['job_class'](id=job.id,
                                          connection=self.connection)
            restored.restore(self.redis_data(job))
            return restored, queue

        with patch.object(Queue, 'dequeue_any', side_effect=dequeue):
            with patch.object(Worker, 'should_run_maintenance_tasks',
                              new_callable=PropertyMock, return_value=False):
                result, _ = worker.dequeue_job_and_maintain_ttl(None)

        messages = '\n'.join(call[0][0].getMessage()
                             for call in handler.emit.call_args_list)
        self.assertIn('create_reads:', messages)
        self.assertIn('ecasa_comer', messages)
        self.assertIn('get_lectures_from_pool_async', messages)
        self.assertIn('context=', messages)
        self.assertIn('sudo=', messages)
        self.assertIn(job.id, messages)
        self.assertNotIn('minio_', messages)
        self.assertNotIn('private-secret', messages)
        self.assertEqual(result.args[0], self.config)

    def test_exception_log_omits_configuration_and_preserves_handler_job(self):
        job = Job.create('oorq.tasks.execute', args=self.args,
                         kwargs=self.kwargs, connection=self.connection)
        worker = Worker.__new__(Worker)
        RQWorker.__init__(worker, [], connection=self.connection,
                          prepare_for_work=False)
        worker.log = logging.Logger('test.worker.exception')
        worker.log.addFilter(Worker.log_filter)
        handler = logging.Handler()
        handler.emit = Mock()
        worker.log.addHandler(handler)
        exception_handler = Mock()
        worker.push_exc_handler(exception_handler)

        try:
            raise ValueError('Task failed')
        except ValueError:
            worker.handle_exception(job, *sys.exc_info())

        records = [call[0][0] for call in handler.emit.call_args_list]
        error = next(record for record in records
                     if record.levelno == logging.ERROR)
        self.assertEqual(error.arguments, self.args[1:])
        self.assertEqual(error.kwargs, self.kwargs)
        self.assertNotIn('private-secret', repr(error.__dict__))
        handled_job = exception_handler.call_args[0][0]
        self.assertIs(handled_job, job)
        self.assertEqual(handled_job.args[0], self.config)
