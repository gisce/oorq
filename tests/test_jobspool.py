# coding=utf-8
import unittest
import uuid

from rq.job import JobStatus

from oorq.oorq import JobsPool


class FakeJob(object):
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.result = None
        self.status = JobStatus.QUEUED

    def get_status(self):
        return self.status


class TestJobsPool(unittest.TestCase):

    def test_num_jobs(self):
        jpool = JobsPool()
        for _ in range(0, 200):
            jpool.add_job(FakeJob())
        self.assertEqual(jpool.num_jobs, 200)

    def test_progress(self):
        jpool = JobsPool()
        for _ in range(0, 200):
            jpool.add_job(FakeJob())
        self.assertEqual(jpool.progress, 0)

        jobs = jpool.pending_jobs[0:100]
        for job in jobs:
            job.status = JobStatus.FINISHED

        self.assertEqual(jpool.progress, 0)

        self.assertEqual(jpool.all_done, False)
        self.assertEqual(jpool.progress, 50)
        self.assertEqual(len(jpool.pending_jobs), 100)
        self.assertEqual(len(jpool.done_jobs), 100)

        for job in jpool.pending_jobs:
            job.status = JobStatus.FINISHED

        self.assertEqual(jpool.all_done, True)
        self.assertEqual(jpool.progress, 100)
        self.assertEqual(len(jpool.pending_jobs), 0)
        self.assertEqual(len(jpool.done_jobs), 200)

    def test_excpetion_raised_when_adding_job_and_is_joined(self):
        jpool = JobsPool()
        for _ in range(0, 200):
            jpool.add_job(FakeJob())
        jpool.joined = True

        with self.assertRaisesRegexp(Exception, "You can't add a job, the pool is joined!"):
            jpool.add_job(FakeJob())

    def test_results(self):
        jpool = JobsPool()
        jobs_ids = []
        for _ in range(0, 200):
            job = FakeJob()
            jobs_ids.append(job.id)
            jpool.add_job(job)

        self.assertEqual(len(jpool.results), 0)
        _ = jpool.all_done
        self.assertEqual(len(jpool.results), 0)

        for job in jpool.pending_jobs[:100]:
            job.status = JobStatus.FINISHED

        _ = jpool.all_done
        self.assertEqual(len(jpool.results), 100)
        self.assertEqual(sorted(jpool.results.keys()), sorted(jobs_ids[:100]))

    def test_failed_as_finished(self):
        jpool = JobsPool()
        for _ in range(0, 200):
            jpool.add_job(FakeJob())
        self.assertEqual(jpool.progress, 0)

        jobs = jpool.pending_jobs[0:100]
        for job in jobs:
            job.status = JobStatus.FAILED

        self.assertEqual(jpool.progress, 0)

        self.assertEqual(jpool.all_done, False)
        self.assertEqual(jpool.progress, 50)
        self.assertEqual(len(jpool.pending_jobs), 100)
        self.assertEqual(len(jpool.done_jobs), 100)

        for job in jpool.pending_jobs:
            job.status = JobStatus.FAILED

        self.assertEqual(jpool.all_done, True)
        self.assertEqual(jpool.progress, 100)
        self.assertEqual(len(jpool.pending_jobs), 0)
        self.assertEqual(len(jpool.done_jobs), 200)
