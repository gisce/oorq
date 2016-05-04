#!/usr/bin/env python
import sys
from redis import from_url

DEFAULT_REDIS_URI = 'redis://localhost:6379/0'

if len(sys.argv) < 2:
    print "RQ Worker killer\n %s worker_name [redis_uri]" % sys.argv[0]
    exit()

worker_name = sys.argv[1]

redis_uri = len(sys.argv) > 2 and sys.argv[2] or DEFAULT_REDIS_URI
redis_conn = from_url(redis_uri)

# Whe expire the key to delete de worker from redis
redis_conn.expire('rq:worker:%s' % worker_name, 0)

