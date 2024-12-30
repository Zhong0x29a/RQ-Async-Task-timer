import hashlib
import os
import time
import uuid
from datetime import timedelta
from time import sleep
from typing import Optional, Union, Callable

from rq import Queue
from rq.job import Job
from rq.types import FunctionReferenceType

from nova_logger import log_event

ARG_DEBUG = os.environ.get('ARG_DEBUG', False)
from db_access.redis_db import Redis4RQ

redis_conn = Redis4RQ.redis_conn

__all__ = ['TaskQueue']


class TaskQueue(object):
	__redis_conn = redis_conn

	def __init__(self, q_name: str = 'default', ):
		self.q_name = q_name
		self.task_queue = Queue(name=self.q_name, connection=self.__redis_conn)

	def add_task(self, func: 'FunctionReferenceType', *args, meta: dict = None,
	             callback_func: Optional[Union[Callable[..., None], 'FunctionReferenceType']] = None,
	             callback_args: tuple = None) -> Job:
		uni_identity = self.__gen_job_id(func, args)
		new_job = self.task_queue.enqueue(func, *args, job_id=uni_identity, meta=meta)

		log_event(f'Immediate Job {new_job.id} added to queue. ')

		if callback_func:
			callback_jid = self.__gen_job_id(callback_func, callback_args)
			callback_job = self.task_queue.enqueue(callback_func,
			                                       *callback_args,
			                                       depends_on=[new_job],
			                                       job_id=callback_jid)
		return new_job

	def add_timer_task(self, exe_in: float, func: 'FunctionReferenceType', *args, meta: dict = None,
	                   callback_func: Optional[Union[Callable[..., None], 'FunctionReferenceType']] = None,
	                   callback_args: tuple = None) -> Job:
		uni_identity = self.__gen_job_id(func, args)
		new_job = self.task_queue.enqueue_in(timedelta(seconds=exe_in), func, *args, job_id=uni_identity, meta=meta)

		log_event(f'Timer Job {new_job.id} added to queue. ')

		if callback_func:
			callback_jid = self.__gen_job_id(callback_func, callback_args)
			callback_job = self.task_queue.enqueue(callback_func,
			                                       *callback_args,
			                                       depends_on=[new_job],
			                                       job_id=callback_jid)
		return new_job

	def add_or_update_timer_task(self, exe_in: float, func: 'FunctionReferenceType', *args, meta: dict = None,
	                             callback_func: Optional[Union[Callable[..., None], 'FunctionReferenceType']] = None,
	                             callback_args: tuple = None) -> Job:
		"""
		!!! WILL OVERWRITE THE PREVIOUS JOB !!!
		Create a uni-task with time delay.
		"""
		uni_identity = self.__gen_job_id(func, args)
		sche_job_ids = self.task_queue.scheduled_job_registry.get_job_ids()
		if uni_identity in sche_job_ids:
			self.task_queue.scheduled_job_registry.remove(uni_identity)

		new_job = self.add_timer_task(exe_in,
		                              func,
		                              *args,
		                              meta=meta,
		                              callback_func=callback_func,
		                              callback_args=callback_args)
		return new_job

	@staticmethod
	def __gen_job_id(func: Optional[Union[Callable[..., None], 'FunctionReferenceType']], *args) -> str:
		try:
			func_hash = hashlib.sha256(f"{func.__module__}.{func.__qualname__}".encode('utf-8')).hexdigest()
			args_hash = hashlib.sha256(str(args).encode('utf-8')).hexdigest()
			return 'JOB_ID_' + str(int(func_hash, 16) + int(args_hash, 16))
		except Exception as e:
			log_event(f'Failed to gen job id, error: {e}')
			return 'JOB_ID_' + str(uuid.uuid4())


''' Test Code '''
if __name__ == '__main__':
	tq = TaskQueue()
	stime = time.time()

	job = tq.add_or_update_timer_task(100,
	                                  taget_fun,
	                                  '....',
	                                  meta={'event_uuid': 'TEST_UUID'})
	sleep(5)
	tq.add_or_update_timer_task(5,
	                            target_fun,
	                            '...',
	                            callback_func=call_back_fun,
	                            callback_args=('done'))
	print('time taken: ', time.time() - stime)
