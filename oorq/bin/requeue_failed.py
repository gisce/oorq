#!/usr/bin/env python
import sys
import times
from redis import from_url
from rq import use_connection, get_failed_queue, requeue_job, Queue

INTERVAL = 7200  # Seconds
MAX_ATTEMPTS = 5
PERMANENT_FAILED = 'permanent'

redis_conn = from_url(sys.argv[1])
use_connection(redis_conn)

fq = get_failed_queue()
pq = Queue(name=PERMANENT_FAILED)

for job in fq.jobs:
    job.meta.setdefault('attempts', 0)
    if job.meta['attempts'] > MAX_ATTEMPTS:
        print "Job %s %s attempts. MAX ATTEMPTS %s limit exceeded on %s" % (
                job.id, job.meta['attempts'], MAX_ATTEMPTS, job.origin
        )
        print job.description
        print job.exc_info
        print
        fq.remove(job)
        pq.enqueue_job(job)
        print "Moved to %s queue" % PERMANENT_FAILED
    else:
        ago = (times.now() - job.enqueued_at).seconds
        if ago >= INTERVAL:
            print "%s: attemps: %s enqueued: %ss ago on %s (Requeue)" % (job.id,
                                                     job.meta['attempts'],
                                                     ago,
                                                     job.origin)
            job.meta['attempts'] += 1
            job.save()
            requeue_job(job.id)
