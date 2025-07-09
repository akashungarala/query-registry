# As a Senior Backend Engineer working at Adara Inc, I designed and developed query registry service which is a python flask
# microservice. When Analytics provides a query for a requested audience, Tech Ops team should be able to use this service to
# set up the audience definition, so that delivery co-ordinator service can send a request to generate the audience. Since the
# audience definition is nothing but a sql query statement to be run in BigQuery, there are certainly some tricky custom
# validation is required so that only valid sql statements to be registered using the POST API and before adding it to the
# registry, we also need to do a dry run execution of the query against BigQuery to confirm the query runs fine.

QUERY_COLLECTION = 'queries_active'
QUERY_INACTIVE_COLLECTION = 'queries_inactive'
QUERY_STATEMENT_COLLECTION = 'query_statement'
ERROR = dict(
    invalid_sql_command='Query statement cannot have the SQL keywords -- DROP, DELETE, UPDATE, REPLACE, ALTER',
    duplicate_parameter_name='Duplicate parameter details found',
    invalid_parameter_data_type='\'{}\' is not one of [\'INT64\', \'NUMERIC\', \'FLOAT64\', \'BOOL\', \'STRING\', '
                                '\'BYTES\', \'DATE\', \'DATETIME\', \'GEOGRAPHY\', \'TIME\', \'TIMESTAMP\']',
    invalid_sample='Invalid format of the parameter sample value',
    missing_parameter_reference='One or more parameters defined are not used in the query statement.',
    query_exists='Query already exists',
    query_does_not_exist='Query does not exist',
    queries_not_found='No queries found in the registry',
    invalid_page_index='Invalid page index value',
    add_query_failure='Failed to add query to the registry'
)

__bq_client__ = None


def create_bq_client():
    """ Convenience function to create a BQ client or pull the client from cache
    :return: an authenticated storage client """
    from google.cloud import bigquery
    global __bq_client__
    if not __bq_client__:
        __bq_client__ = bigquery.Client()
    return __bq_client__


def add_query(summary: str, description: str, query: str, parameters: list = None):
    """ Convenience function to add a new query to the registry
    :param summary: summary for an audience that helps understanding what it does
    :param description: detailed description that helps understanding the results to expect
    :param query: sql query statement
    :param parameters: (Optional) list of dictionary objects referred in the query statement (each object has parameter
    detail keys - name, type, value)
    :return: complete query details - query id, summary, description, query, parameters, created, updated """
    query_details = None
    # Validate query statement and parameters
    query, error = is_valid_query(query=query, parameters=parameters)
    if not error:
        # Generate id for the query statement
        query_statement_id = generate_query_statement_id(query=query)
        # Validate for query existence
        query_statement_details, error = fetch_query_statement_details(
            query_statement_id=query_statement_id)
        if not error:
            query_id = query_statement_details.get('query_id')
            query_details, error = fetch_query_details(query_id=query_id)
            if not error:
                del query_details['query_statement_id']
                error = ERROR['query_exists']
            else:
                query_details, error = create_query(query_statement_id=query_statement_id, summary=summary,
                                                    description=description, query=query, parameters=parameters)
        else:
            query_details, error = create_query(query_statement_id=query_statement_id, summary=summary,
                                                description=description, query=query, parameters=parameters)
    return query_details, error


def create_query(query_statement_id: str, summary: str, description: str, query: str, parameters: list):
    """ Convenience function to create a new query and add it to the registry """
    from firestore import create_db_client
    from datetime import datetime
    query_details = None
    db = create_db_client()
    query_reference = db.collection(QUERY_COLLECTION).document()
    query_id = query_reference.id
    if not parameters:
        parameters = list()
    created = int(datetime.utcnow().timestamp())
    updated = created
    query_data = {'query_id': query_id, 'query_statement_id': query_statement_id, 'summary': summary,
                  'description': description, 'parameters': parameters, 'created': created, 'updated': updated}
    add_data(collection_name=QUERY_COLLECTION,
             document_name=query_id, data=query_data)
    query_statement_data = {'query_id': query_id, 'query_statement_id': query_statement_id, 'query': query,
                            'parameters': parameters}
    add_data(collection_name=QUERY_STATEMENT_COLLECTION,
             document_name=query_statement_id, data=query_statement_data)
    details, error = fetch_query_details(query_id=query_id)
    if not error:
        query_details = details
        del query_details['query_statement_id']
    else:
        error = ERROR['add_query_failure']
    return query_details, error


def is_valid_query(query: str, parameters: list):
    """ Convenience function to validate and then dry run a parameterized query against BQ tables, datasets"""
    error = None
    if not check_for_invalid_keywords(query=query):
        error = ERROR['invalid_sql_command']
    query_params = None
    if not error and parameters:
        query, query_params, error = fetch_valid_query_params(
            query=query, parameters=parameters)
    if not error:
        error = dry_run_query(query=query, query_params=query_params)
    return query, error


def check_for_invalid_keywords(query: str) -> bool:
    """ Convenience function to  validate query statement for invalid sql keywords """
    invalid_sql_keywords = {'DROP ', 'DELETE ',
                            'UPDATE ', 'REPLACE ', 'ALTER '}
    for command in invalid_sql_keywords:
        if command.lower() in query.lower():
            return False
    return True


def fetch_valid_query_params(query: str, parameters: list):
    """ Convenience function to check if all the parameters has valid details """
    query_params = list()
    error = None
    names_appeared = set()
    for parameter in parameters:
        query, query_param, error = fetch_valid_parameter(
            query=query, parameter=parameter)
        if error:
            break
        name = parameter.get('name')
        # Check if the parameter appeared already
        if name in names_appeared:
            query_params = list()
            error = ERROR['duplicate_parameter_name']
            break
        names_appeared.add(name)
        if not error:
            query_params.append(query_param)
    if not error and len(query_params) != len(parameters):
        query_params = list()
        error = ERROR['invalid_parameter']
    return query, query_params, error


def fetch_valid_parameter(query: str, parameter: dict):
    """ Convenience function to check if the parameter has valid details """
    from datetime import date, datetime, time
    query_param = None
    error = None
    name = parameter.get('name')
    data_type = parameter.get('type')
    sample = parameter.get('sample')
    # Check if the parameter is referred in the query text
    if not error and '@{}'.format(name) not in query:
        error = ERROR['missing_parameter_reference']
    if not error and isinstance(sample, list):
        query, error = modify_query_with_array_parameters(
            query=query, name=name)
    if not error:
        valid_bq_data_types = {'INT64': int, 'NUMERIC': float, 'FLOAT64': float, 'BOOL': bool, 'STRING': str,
                               'BYTES': int, 'DATE': date, 'DATETIME': datetime, 'GEOGRAPHY': str, 'TIME': time,
                               'TIMESTAMP': datetime}
        bq_type = valid_bq_data_types.get(data_type)
        # Check for valid standard SQL data type
        if not error and not bq_type:
            error = ERROR['invalid_parameter_data_type'].format(data_type)
        # Check if the sample value (or array of values) match with the data type
        if not error:
            query_param, error = fetch_query_param(
                name=name, data_type=data_type, sample=sample, bq_type=bq_type)
    return query, query_param, error


def modify_query_with_array_parameters(query: str, name: str):
    """ Convenience function to modify query statement when an array type parameter is referred """
    if query.count('UNNEST(@{})'.format(name)) != query.count('@{}'.format(name)):
        query = ' '.join(query.split())
        for search_phrase in ['UNNEST (@{})', 'UNNEST(@{})', '(@{})']:
            query = query.replace(
                search_phrase.format(name), '@{}'.format(name))
        query = query.replace('@{}'.format(name), 'UNNEST(@{})'.format(name))
    error = None if query.count('UNNEST(@{})'.format(name)) == query.count('@{}'.format(name)) else \
        "Please make sure that @{} parameter reference is syntactically correct".format(
            name)
    return query, error


def fetch_query_param(name: str, data_type: str, sample: any, bq_type: any):
    """ Convenience function to check if the sample value (or array of values) match with the data type and return bq
     query param """
    from google.cloud import bigquery
    query_param = None
    error = None
    if isinstance(sample, bq_type):
        query_param = bigquery.ScalarQueryParameter(name, data_type, sample)
    elif isinstance(sample, list):
        for value in sample:
            if not isinstance(value, bq_type):
                error = ERROR['invalid_sample']
                break
        if not error:
            query_param = bigquery.ArrayQueryParameter(name, data_type, sample)
    else:
        error = ERROR['invalid_sample']
    return query_param, error


def dry_run_query(query: str, query_params: list = None):
    """ Convenience function to dry run the parameterized query against BQ tables/datasets """
    from google.cloud.bigquery import QueryJobConfig
    from google.cloud.exceptions import NotFound, Forbidden, BadRequest, GrpcRendezvous, Unauthorized
    from server import logger
    error = None
    job_config = QueryJobConfig(use_legacy_sql=False, dry_run=True)
    if query_params:
        job_config.query_parameters = query_params
    client = create_bq_client()
    try:
        client.query(
            query, location='US', job_config=job_config)
    except NotFound as exception:
        logger.warn(exception)
        error = exception.args[0].split(
            '/jobs?prettyPrint=false: Not found: ')[1]
    except BadRequest as exception:
        logger.warn(exception)
        error = exception.args[0].split('/jobs?prettyPrint=false: ')[1]
        if 'Unrecognized name: ' in error:
            error = error.split("Unrecognized name: ")[1].split()[
                0] + ' is not a valid column of the table'
    except Forbidden as exception:
        logger.warn(exception)
        error = 'Query Service does not have access permission. ' + \
            exception.args[0].split('/jobs: ')[1]
    except Unauthorized as exception:
        logger.warn(exception)
        error = "Query Service is not authorized to make this request. " + \
            exception.args[0].split('/jobs: ')[1]
    except GrpcRendezvous as exception:
        logger.warn(exception)
        error = exception.args[0].split('/jobs?prettyPrint=false: ')[1]
    return error


def fetch_query_details(query_id: str):
    """ Convenience function to fetch the complete query details from the registry
    :param query_id: unique identifier which is generated while adding the query to the registry
    :param active: status flag - True if the query is active to use, False if the query has been deleted
    :return: complete query details - query id, query statement id, summary, description, query, parameters, created,
    updated """
    details = None
    collection_name = QUERY_ACTIVE_COLLECTION if active else QUERY_INACTIVE_COLLECTION
    query_details, error = fetch_document_data(
        collection_name=collection_name, document_name=query_id)
    if not error:
        query_statement_id = query_details.get('query_statement_id')
        query_statement_details, error = fetch_document_data(collection_name=QUERY_STATEMENT_COLLECTION,
                                                             document_name=query_statement_id)
        if not error:
            details = query_details
            details['query'] = query_statement_details.get('query')
    return details, error


def fetch_query_statement_id(query_id: str, active=True):
    """ Convenience function to fetch unique identifier of the query statement from the registry
    :param query_id: unique identifier which is generated while adding the query to the registry
    :param active: status flag - True if the query is active to use, False if the query has been deleted
    :return: unique identifier of the query statement which is generated while adding the query to the registry """
    query_statement_id = None
    collection_name = QUERY_ACTIVE_COLLECTION if active else QUERY_INACTIVE_COLLECTION
    details, error = fetch_document_data(
        collection_name=collection_name, document_name=query_id)
    if not error:
        query_statement_id = details.get('query_statement_id')
    return query_statement_id, error


def fetch_query_statement_details(query_statement_id: str):
    """ Convenience function to fetch the query statement details from the registry
    :param query_statement_id: unique identifier of the query statement which is generated while adding the query to
    the registry
    :return: query statement details - query id, query statement id, query, parameters """
    return fetch_document_data(collection_name=QUERY_STATEMENT_COLLECTION, document_name=query_statement_id)


def fetch_queries_list(page: int, size=25, active=True):
    """ Convenience function to fetch the details of all the queries stored in the registry
    :param page: page number required to fetch the paginated results
    :param size: paginated result count of the query details fetched
    :param active: status flag - True if the query is active to use, False if the query has been deleted
    :return: list of query details - query id, summary, description, parameters, created, updated """
    queries_list = list()
    if page < 1:
        error = ERROR['invalid_page_index']
        return queries_list, error
    query_docs, error = fetch_query_docs(page=page, size=size, active=active)
    for query_doc in query_docs:
        query_details = query_doc.to_dict()
        query_statement_id = query_details.get('query_statement_id')
        if query_statement_id:
            del query_details['query_statement_id']
            queries_list.append(query_details)
    return queries_list, error


def fetch_query_docs(page: int, size: int, active: bool, order_by='created', direction='DESCENDING'):
    """ Convenience function to fetch paginated query documents stored in the registry """
    from firestore import create_db_client
    from google.cloud.exceptions import NotFound
    from server import logger
    query_docs = None
    error = None
    db = create_db_client()
    collection_name = QUERY_ACTIVE_COLLECTION if active else QUERY_INACTIVE_COLLECTION
    query_collection_ref = db.collection(
        collection_name).order_by(order_by, direction=direction)
    if page == 1:
        try:
            query_docs = query_collection_ref.limit(size).get()
        except NotFound:
            logger.warn(NotFound)
            error = ERROR['queries_not_found']
    else:
        previous_docs_limit = size * (page - 1)
        try:
            previous_docs = query_collection_ref.limit(
                previous_docs_limit).get()
        except NotFound:
            logger.warn(NotFound)
            error = ERROR['queries_not_found']
        else:
            last_doc = list(previous_docs)[-1]
            query_docs = query_collection_ref.start_after(
                last_doc).limit(size).get()
    return query_docs, error


def generate_query_statement_id(query: str) -> str:
    """ Convenience function to generate md5 hashcode for the query statement
    :param query: sql query statement
    :return: unique query statement identifier """
    import hashlib
    return str(hashlib.md5(query.encode('utf-8')).hexdigest())


def fetch_document_data(collection_name: str, document_name: str):
    """ Convenience function to fetch document data in the collection """
    from firestore import create_db_client
    from google.cloud.exceptions import NotFound
    from server import logger
    data = None
    error = None
    db = create_db_client()
    try:
        query_doc = db.collection(
            collection_name).document(document_name).get()
    except NotFound:
        logger.warn(NotFound)
        error = ERROR['query_does_not_exist']
    else:
        if query_doc.exists:
            data = query_doc.to_dict()
        else:
            error = ERROR['query_does_not_exist']
    return data, error


def add_data(collection_name: str, document_name: str, data: dict):
    """ Convenience function to add document data in the collection """
    from firestore import create_db_client
    db = create_db_client()
    db.collection(collection_name).document(document_name).set(data)
