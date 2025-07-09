# As a Senior Backend Engineer working at Adara Inc, I designed and developed delivery coordinator service which is a python flask
# microservice. When there is an API request for a delivery, the different jobs required for an audience delivery should be
# coordinated, so all the requested data for a specific client on a given destination can be delivered. There are a few tricky
# scenarios I encountered while designing and implementing this service. Since this service initiates delivery which involves looking
# up the last timestamp it ran and at what time in the future it will be eligible again to be run. This should be an automated
# process like it periodically finds the eligible deliveries and initiate runs optimally in equal batches throughout the day.

# To solve these challenges, I designed a scheduler to run a celery task for every 30 mins and calculate the optimal batch size at
# that point of time with the information of how many eligible deliveries to be run in the next 24 hours window.

# Celery Worker - Asynchronous queue for task management (Promise Handler)

# A job is created when a delivery is initiated to run then for every query listed in the delivery document, an api call is made to
# the query registry service which inturn creates a job in query service and returns the job_id back which becomes a promise to
# the job in delivery service. Job in the query service will be successful eventually when BigQuery completes the query run. Celery
# worker being the promise handler, updates the job with the status change of the promise so that the next task is triggered to go
# ahead to the next step of making an api call to distribution service as a new promise. This process goes on until all the steps
# get completed and at the end, job status is changed to success and the delivery document is also updated with this info to know when
# it will be eligible to run for the next time


from server import celery_app

DELIVERY_COLLECTION = 'deliveries'
SCHEDULER_FREQUENCY_IN_HOURS = 24
DELIVERY_EXECUTION_SCHEDULED_FOR_SECONDS = 60.0 * 30
MIN_THRESHOLD_SLOT_SIZE = 2

# below is the example code snippet how I defined the celery settings to include beat schedule for my scheduler task and created
# celery_app in server.py
celery_settings = dict(
    accept_content=['json'],
    result_serializer='json',
    beat_schedule={
        'process-files-hourly': {
            'task': 'tasks.delivery.collect_active_deliveries',
            'schedule': DELIVERY_EXECUTION_SCHEDULED_FOR_SECONDS,
        }
    },
    timezone='UTC'
)

# below are the tasks defined in the file tasks.delivery.py


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def create_execution_job(job_id: str, delivery: dict, update_next_run: bool):
    return fetch_delivery_from_registry(job_id=job_id, delivery=delivery, update_next_run=update_next_run)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def deliver_query_segments(job_id: str, distribution_details: dict):
    return deliver_segments(job_id=job_id, distribution_details=distribution_details)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def publish_delivery_status(job_id: str):
    return publish_status(job_id=job_id)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def collect_active_deliveries():
    return execute_deliveries()


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def execute_active_delivery(delivery_id: str, update_next_run=True):
    return execute_delivery(delivery_id=delivery_id, update_next_run=update_next_run)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def execute_query_failure_task(job_id: str, promise_id: str, attempts: int, segment, segment_details_list):
    return execute_query(job_id=job_id, attempts=attempts, segment=segment, segment_details_list=segment_details_list)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def execute_audience_failure_task(job_id: str, promise_id: str, attempts: int, segment, segment_details_list, dp_ids):
    return execute_audience(job_id=job_id, attempts=attempts, segment=segment,
                            segment_details_list=segment_details_list, dp_ids=dp_ids)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def execute_distribution_failure_task(job_id: str, promise_id: str, attempts: int, distribution_details):
    return deliver_segments(job_id=job_id, attempts=attempts, distribution_details=distribution_details)


@celery_app.task(autoretry_for=(Exception,), retry_backoff=True)
def execute_failure_task(job_id: str, promise_id=None, attempts=None, max_attempts=None):
    return publish_failure_status(job_id=job_id, promise_id=promise_id)


def execute_deliveries():
    """ Convenience function to fetch the list of executable active deliveries and execute them
    :return: error message """
    from firestore import get_all_documents
    from tasks.delivery import execute_active_delivery
    all_deliveries = get_all_documents(collection_name=DELIVERY_COLLECTION, eq_scheduled_to_run=True,
                                       sort_keys=['next_run_ascending'])
    deliveries = filter(is_executable, all_deliveries)
    batch_size = get_batch_size(delivery_count=len(deliveries))
    for delivery in deliveries[:batch_size]:
        execute_active_delivery.delay(delivery_id=delivery['id'])


def is_executable(delivery: dict):
    from datetime import datetime
    now = datetime.utcnow()
    if is_scheduled_end_valid(delivery=delivery, now=now):
        return validate_last_run_next_run(delivery=delivery, now=now)
    return False


def validate_last_run_next_run(delivery: dict, now):
    from datetime import timedelta
    last_run = delivery.get('last_run')
    next_run = delivery['next_run']
    next_run_timestamp = convert_timestamp_with_nanoseconds_to_datetime(
        timestamp_with_nanoseconds=next_run)
    if not last_run or next_run_timestamp <= now + timedelta(hours=SCHEDULER_FREQUENCY_IN_HOURS):
        return True
    return False


def is_scheduled_end_valid(delivery: dict, now):
    from firestore import update_document
    delivery_id = delivery['id']
    schedule_start = delivery['schedule_start']
    schedule_end = delivery['schedule_end']
    is_valid = False
    enable_scheduled_to_run = False
    if (isinstance(schedule_start, str) and isinstance(schedule_end, str)
            and convert_string_to_datetime(date=schedule_start) <= now):
        if convert_string_to_datetime(date=schedule_end) <= now:
            enable_scheduled_to_run = True
        else:
            is_valid = True
    if enable_scheduled_to_run:
        update_document(collection_name=DELIVERY_COLLECTION, document_id=delivery_id,
                        attributes=dict(scheduled_to_run=False))
    return is_valid


def get_batch_size(delivery_count: int):
    import math
    if delivery_count <= MIN_THRESHOLD_SLOT_SIZE:
        batch_size = delivery_count
    else:
        frequency_in_seconds = DELIVERY_EXECUTION_SCHEDULED_FOR_SECONDS
        number_of_batches = (SCHEDULER_FREQUENCY_IN_HOURS *
                             60 * 60) / frequency_in_seconds
        batch_size = math.ceil(delivery_count / number_of_batches)
        if batch_size < MIN_THRESHOLD_SLOT_SIZE:
            batch_size = MIN_THRESHOLD_SLOT_SIZE
    return batch_size


def execute_delivery(delivery_id: str, update_next_run: bool):
    """ Convenience function to execute delivery which is valid and ready
    :param delivery_id: executable active delivery id
    :param update_next_run: flag for updating next run timestamp
    :return: error message """
    delivery, error = get_delivery(delivery_id=delivery_id)
    if not error and delivery and validate_last_run_next_run(delivery=delivery):
        run_delivery(delivery=delivery, update_next_run=update_next_run)
    return error


def get_delivery(delivery_id: str):
    """ Convenience function to fetch the existing delivery document from the registry
    :param delivery_id: unique identifier of a delivery in the registry
    :return: existing delivery document and the error message """
    from firestore import get_documents
    delivery = dict()
    error = None
    deliveries = get_documents(
        collection_name=DELIVERY_COLLECTION, eq_id=delivery_id)
    if deliveries:
        delivery = deliveries[0]
    else:
        error = f'Delivery with id: {delivery_id} is not found'
    return delivery, error


def run_delivery(delivery: dict = None, update_next_run=True):
    """ Convenience function to handle the request for delivery execution
    :param delivery: details of the existing delivery
    :param update_next_run: flag for updating next run timestamp
    :return: job id and created timestamp """
    from tasks.delivery import create_execution_job
    from firestore import generate_random_id
    from datetime import datetime
    job_id = generate_random_id()
    created = datetime.utcnow()
    create_execution_job.delay(
        job_id=job_id, delivery=delivery, update_next_run=update_next_run)
    return job_id, created


def fetch_delivery_from_registry(job_id: str, delivery: dict, update_next_run: bool):
    """ Convenience function to fetch delivery from the registry
    :param job_id: unique job identifier
    :param delivery: details of the existing delivery
    :param update_next_run: flag for updating next run timestamp
    :return: job id and the list of promises """
    promises = list()
    # TODO: Make api calls to query registry service to kick start the execution of queries listed in the delivery document
    # and every api call to query registry service returns a job_id_from_query_service as a new job is created for query
    # service which becomes the promise_id for the current job in delivery service
    return job_id, promises


def convert_string_to_datetime(date: str):
    """ Convenience function to convert a date string into a datetime value
    :param date: date string value
    :return: datetime value """
    from datetime import datetime
    return datetime.strptime(date, '%Y-%m-%d')


def convert_timestamp_with_nanoseconds_to_datetime(timestamp_with_nanoseconds):
    """ Convenience function to convert Google timestamp with nanoseconds into a datetime value
    :param timestamp_with_nanoseconds: Google timestamp with nanoseconds value
    :return: datetime value """
    from google.api_core.datetime_helpers import from_rfc3339, DatetimeWithNanoseconds
    from datetime import datetime
    timestamp_rfc3339 = DatetimeWithNanoseconds.rfc3339(
        timestamp_with_nanoseconds)
    return datetime.utcfromtimestamp(int(from_rfc3339(timestamp_rfc3339).timestamp()))
