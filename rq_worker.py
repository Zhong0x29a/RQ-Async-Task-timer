import os

from redis import Redis, ConnectionPool
from rq import Worker, Queue
from rq.job import Job

"""
Essential packages
"""
...
# noinspection PyUnresolvedReferences
from utils import *

ARG_DEBUG = os.environ.get('ARG_DEBUG', False)
if ARG_DEBUG:
	__redis_pool = ConnectionPool(host='localhost', port=6379, db=0)
	redis_conn = Redis(connection_pool=__redis_pool)
else:
	from db_access.redis_db import Redis4RQ
	redis_conn = Redis4RQ.redis_conn

__all__ = ['MyWorker']


class MyWorker(Worker):
	def execute_job(self, job: 'Job', queue: 'Queue'):
		__event_uuid = job.meta.get('event_uuid', "SYSTEM_EVENT")
		event_uuid.set(__event_uuid)
		super().execute_job(job, queue)


q_names = ['default']
if __name__ == '__main__':
	que_list = [Queue(name, connection=redis_conn) for name in q_names]
	worker = MyWorker(que_list)

	# Start Worker
	worker.work(with_scheduler=True)


exit()