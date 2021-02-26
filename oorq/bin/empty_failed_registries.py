import sys

from redis import from_url
from rq import use_connection, Queue
from rq.registry import FailedJobRegistry

# Script to empty the failed jobs from all registries.

CHUNK_SIZE = 10000

redis_conn = from_url(sys.argv[1])
use_connection(redis_conn)
all_queues = Queue().all()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


for queue in all_queues:
    print("Queue: {}".format(queue.name))
    fq = FailedJobRegistry(queue.name)
    all_reg_jobs = fq.get_job_ids()
    print("Failed jobs: {}".format(fq.count))
    for jobs_id_chunk in chunks(all_reg_jobs, CHUNK_SIZE):
        jobs_id_redis_chunk = [
            'rq:job:{}'.format(job_id) for job_id in jobs_id_chunk
        ]
        redis_conn.delete(*jobs_id_redis_chunk)
        redis_conn.zrem(fq.key, *jobs_id_chunk)