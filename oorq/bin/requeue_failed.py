#!/usr/bin/env python
#Example
# python requeue_failed.py redis://localhost/0 --max-attempts 20 --interval 3500

from __future__ import print_function
import argparse
import times
from redis import from_url
from rq import use_connection, requeue_job, Queue
from rq.job import Job
from rq.registry import FailedJobRegistry

INTERVAL = 7200  # Seconds
MAX_ATTEMPTS = 5
PERMANENT_FAILED = 'permanent'


def main(redis_conn, interval, max_attempts, permanent_failed):
    use_connection(redis_conn)

    all_queues = Queue().all()
    pfq = FailedJobRegistry(permanent_failed)
    pq = Queue(name=permanent_failed)
    print("Try to requeu jobs")
    for queue in all_queues:
	if queue.name == 'jobspool-autoworker':
	    continue
	fq = FailedJobRegistry(queue.name)
	for job_id in fq.get_job_ids():
	    try:
		job = Job.fetch(job_id)
	    except:
		print("Job {} not exist anymore. We will delete from FailedJobRegistry".format(job_id))
		try:
		    key_registry = fq.key
		    redis_conn.zrem(key_registry,job_id)
		except Exception as e:
		    print("We cannot delete job in FailedJobRegistry")
		    print(job_id)
		    print(e)
	    if not job.meta.get('requeue', True):
		continue
	    job.meta.setdefault('attempts', 0)
	    if job.meta['attempts'] > max_attempts:
		print("Job %s %s attempts. MAX ATTEMPTS %s limit exceeded on %s" % (
			job.id, job.meta['attempts'], max_attempts, job.origin
		))
		print(job.description)
		print(job.exc_info)
		print()
		fq.remove(job)
		pq.enqueue_job(job)
		print("Moved to %s FailedJobRegistry" % permanent_failed)
	    else:
		ago = (times.now() - job.enqueued_at).seconds
		if ago >= interval:
		    print("%s: attemps: %s enqueued: %ss ago on %s (Requeue)" % (
			job.id, job.meta['attempts'], ago, job.origin
		    ))
		    job.meta['attempts'] += 1
		    job.save()
		    requeue_job(job.id, connection=redis_conn)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description="Requeue failed jobs"
    )

    parser.add_argument(
        'redis_conn',
        type=str,
        help="Connection address to Redis",
    )
    parser.add_argument(
        '--interval',
        dest='interval',
        default=INTERVAL,
        type=int,
        help="Interval before requeu (in seconds)",
    )
    parser.add_argument(
        '--max-attempts',
        dest='max_attempts',
        default=MAX_ATTEMPTS,
        type=int,
        help="Max attemps before move to permanent failed",
    )
    parser.add_argument(
        '--permanent',
        dest='permanent',
        default=PERMANENT_FAILED,
        type=str,
        help="Name of permanent failed queue",
    )

    args = parser.parse_args()
    main(from_url(args.redis_conn), args.interval, args.max_attempts, args.permanent)

# vim: et ts=4 sw=4
