#!/usr/bin/env python
import sys
import times
from redis import from_url
from rq import use_connection, get_failed_queue, requeue_job, Queue, get_current_job

INTERVAL = 7200  # Seconds
MAX_ATTEMPTS = 5
PERMANENT_FAILED = 'permanent'

redis_conn = from_url(sys.argv[1])
use_connection(redis_conn)

#fq = get_current_job
profiling_queue = Queue(name='profiling')
pending_cofs_queue = Queue(name='profiling_pending_cofs')

for job in pending_cofs_queue.jobs:
    job.meta.setdefault('attempts', 0)
    if False:
    #if job.meta['attempts'] > MAX_ATTEMPTS:
        print "Job %s %s attempts. MAX ATTEMPTS %s limit exceeded on %s" % (
                job.id, job.meta['attempts'], MAX_ATTEMPTS, job.origin
        )
        print job.description
        print job.exc_info
        print
        #q.remove(job)
        pq.enqueue_job(job)
        print "Moved to %s queue" % PERMANENT_FAILED
    if False:
        ago = (times.now() - job.enqueued_at).seconds
        if ago >= INTERVAL:
            print "%s: attemps: %s enqueued: %ss ago on %s (Requeue)" % (job.id,
                                                     job.meta['attempts'],
                                                     ago,
                                                     job.origin)
            job.meta['attempts'] += 1
            job.save()
            requeue_job(job.id)
    profiling_queue.enqueue_job(job)
