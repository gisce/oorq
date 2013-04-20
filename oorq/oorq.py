# -*- coding: utf-8 -*-
from rq import Connection, Queue
from redis import Redis
from task import execute
import time

# Tell RQ what Redis connection to use
redis_conn = Redis()
q = Queue(connection=redis_conn)  # no args implies the default queue

# Delay calculation of the multiplication
job = q.enqueue(execute, 'openerp', 1, 'res.partner', 'search', [])

# Now, wait a while, until the worker is finished
res = job.result
while not res:
    print '.',
    time.sleep(0.1)
    res = job.result
print res

#from osv.osv import osv_pool
