#!/usr/bin/env python
import sys
from redis import from_url
from rq import use_connection, Queue

redis_conn = from_url(sys.argv[1])
use_connection(redis_conn)

profiling_queue = Queue(name='profiling')
fix_cch_tm_queue = Queue(name='fix_cch_tm')
profiling_pending_cofs_queue = Queue(name='profiling_pending_cofs')
fix_cch_tm_pending_cofs_queue = Queue(name='pending_cofs_fix_cch_tm')

for job in profiling_pending_cofs_queue.jobs:
    profiling_pending_cofs_queue.remove(job)
    profiling_queue.enqueue_job(job)
    print "Moved to %s queue" % profiling_queue

for job in fix_cch_tm_pending_cofs_queue.jobs:
    fix_cch_tm_pending_cofs_queue.remove(job)
    fix_cch_tm_queue.enqueue_job(job)
    print "Moved to %s queue" % fix_cch_tm_queue
