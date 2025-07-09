# As a Senior Backend Engineer working at Adara Inc, I implemented custom functionality and packaged it to handle various tricky
# scenarios while working with Google Firestore so that any of our Python flask microservices could use them directly by importing
# from the package. Firestore is a NoSQL document database built for automatic scaling, high performance, and ease of application
# development.

# def manage_retries -> most frequently used to automatically retry for a specific number of attempts to fetch results from a given
# function when failure happens and it lets you provide how to handle specific exceptions that you expect

# def get_documents -> most frequently used functionality to fetch filtered list of documents formatted as dictionaries.
# It can take parameters like collection_name, limit (default to 10), cursor_id (for paginated response),
# create_cursor (to set cursor_id for the first page), sort_keys (list of fields to sort by either asc or desc),
# page (page number), **kwargs (any number of filter options (like field_name=value) you want to apply)

# def get_all_documents -> to fetch all the documents after applying filters and sorting options that are passed in

# def update_document -> most frequently used functionality to update the document by id with the updated attributes (passed in
# as a dictionary). It supports batch processing which means you can commit a batch of document updates at once and lets you choose
# if you need to overwrite the entire document with the provided attributes

# def delete_document -> to delete the document by id. It supports batch processing which means you can commit a batch of document
# deletes at once

# def iterate -> to fetch and iterate over a huge collection of documents by using yield (generators) and process handling each
# document right when it is fetched. It is most commonly used in scripts to automatically generate reports based on the status or
# any specific logic to provide requested information to the technical support team

__db_client__ = None

PROJECT_ID = ""
DB_NAME = ""


def create_db_client():
    """ Convenience function to create a firestore client
    :return: firestore client """
    from google.cloud import firestore
    global __db_client__
    if not __db_client__:
        __db_client__ = firestore.Client(project=PROJECT_ID, database=DB_NAME)
    return __db_client__


def generate_random_id():
    """ Generate a random id
    :return: a unique UUID4 formatted string """
    import uuid
    return str(uuid.uuid4())


def manage_retries(partial_function, handled_exceptions, propagate_exceptions, retries, backoff=True):
    from time import sleep
    success = False
    attempts = 0
    results = None
    delay = 1
    while attempts < retries and not success:
        try:
            results = partial_function()
        except handled_exceptions as e:
            attempts += 1
            if retries == attempts and propagate_exceptions:
                raise e
            else:
                # logger.warning(e)
                if backoff:
                    sleep(delay)
                    delay *= 2
        else:
            success = True
    return results


def get_documents(collection_name, limit=10, cursor_id=None, create_cursor=False, sort_keys: list = None, page=None, **kwargs):
    from google.cloud.exceptions import ServiceUnavailable
    db = create_db_client()
    doc_ref = db.collection(collection_name)
    doc_ref = apply_filters(doc_ref=doc_ref, filters=kwargs)
    doc_ref = handle_sort(doc_ref=doc_ref, sort_keys=sort_keys)
    doc_ref = doc_ref.limit(limit)
    # set page value when cursor_id is passed in
    if cursor_id:
        cursor = get_cursor(cursor_id=cursor_id,
                            sort_keys=sort_keys, filters=filters)
        if not cursor.get('last_document'):
            return [], None
        else:
            page = cursor['page']
    # set offset when page is passed in or set
    if page is not None:
        doc_ref = doc_ref.offset(limit * page)
    # fetch documents (attempt until successful upto 3 times before raising ServiceUnavailable exception)
    documents = manage_retries(partial_function=doc_ref.get, handled_exceptions=[ServiceUnavailable],
                               propagate_exceptions=True, retries=3)
    # update cursor and return documents
    if cursor_id or create_cursor:
        if page is None:
            page = 0
        last_document = documents[-1] if documents else None
        cursor_id = update_cursor(collection_name=collection_name, sort_keys=sort_keys, filters=filters,
                                  page=page + 1, last_document=last_document, cursor_id=cursor_id)
        if limit is None or not documents or len(documents) < limit:
            cursor_id = None
    else:
        cursor_id = None
    return [document.to_dict() for document in documents], cursor_id


def update_cursor(collection_name, sort_keys, filters, page=0, last_document=None, cursor_id=None):
    attributes = dict(page=page)
    attributes['last_document'] = last_document.to_dict(
    ) if last_document else None
    if not cursor_id:
        cursor_id = generate_random_id()
        attributes['collection_name'] = collection_name
        attributes['query_attributes'] = filters
        attributes['sort_keys'] = sort_keys
    db = create_db_client()
    db.collection('cursors').document(cursor_id).set(attributes, merge=True)
    return cursor_id


def get_cursor(cursor_id, sort_keys, filters):
    db = create_db_client()
    cursor = db.collection('cursors').document(cursor_id).get().to_dict()
    if not cursor:
        raise ValueError("Cursor does not exist")
    if cursor['query_attributes'] != filters or cursor['sort_keys'] != sort_keys:
        raise ValueError("Cursor does not match provided query")
    return cursor


def apply_filters(doc_ref, filters):
    operation_map = dict(
        contains='array_contains',
        contains_any='array_contains_any',
        eq_in='in',
        eq='==',
        lt='<',
        lte='<=',
        gt='>',
        gte='>='
    )
    for key, value in filters.items():
        parts = key.split('_')
        if key.startswith('eq_in_') or key.startswith('contains_any_'):
            attribute = '_'.join(parts[2:])
            operation = operation_map['_'.join(parts[:2])]
        else:
            attribute = '_'.join(parts[1:])
            operation = operation_map[parts[0]]
        doc_ref = doc_ref.where(attribute, operation, value)
    # For example:
    # filters = {                           # parts                           key                     value         code executed   attribute     operation
    #     "eq_in_us_regions": "east_coast", # ["eq", "in", "us", "regions"]   "eq_in_us_regions"      "east_coast"  if block        "us_regions"  "in"
    #     "contains_any_numbers": [23, 34], # ["contains", "any", "numbers"]  "contains_any_numbers"  [23, 34]      if block        "numbers"     "array_contains_any"
    #     "eq_is_active": True,             # ["eq", "is", "active"]          "eq_is_active"          True          else block      "is_active"   "=="
    #     "contains_ids": "abc",            # ["contains", "ids"]             "contains_ids"          "abc"         else block      "ids"         "array_contains"
    #     "lte_total": 4                    # ["lte", "total"]                "lte_total"             4             else block      "total"       "<="
    # }
    return doc_ref


def handle_sort(doc_ref, sort_keys):
    if sort_keys:
        for key in sort_keys:
            # For example: when key is "created_at_descending"
            parts = key.split('_')  # parts -> ["created", "at", "descending"]
            field_path = '_'.join(parts[:-1])  # field_path -> "created_at"
            direction = parts[-1].upper()  # direction -> "DESCENDING"
            doc_ref = doc_ref.order_by(
                field_path=field_path, direction=direction)
    return doc_ref


def get_all_documents(collection_name: str, limit=1000, **kwargs):
    all_documents = list()
    documents, cursor_id = get_documents(
        collection_name=collection_name, limit=limit, create_cursor=True, **kwargs)
    if documents:
        all_documents.extend(documents)
    while len(documents) == limit:
        documents, cursor_id = get_documents(collection_name=collection_name, limit=limit, create_cursor=True,
                                             cursor_id=cursor_id, **kwargs)
        if documents:
            all_documents.extend(documents)
    return all_documents


def format_update_message(attributes):
    update = dict()
    for key1, value1 in attributes.items():
        if isinstance(value1, dict):
            for key2, value2 in format_update_message(value1).items():
                update[f'{key1}.{key2}'] = value2
        else:
            update[key1] = value1
    return update


def update_document(collection_name: str, document_id, attributes: dict, batch=None, upsert=False, override=False, overwrite_updated=False):
    from datetime import datetime
    from copy import deepcopy
    db = create_db_client()
    doc_ref = db.collection(collections[collection_name]).document(document_id)
    now = datetime.utcnow()
    attributes = deepcopy(attributes)
    if not overwrite_updated:
        attributes['updated'] = now
    if not override:
        attributes = format_update_message(attributes=attributes)
    if batch:
        batch.set(doc_ref, attributes, merge=True) if upsert else batch.update(
            doc_ref, attributes)
    else:
        doc_ref.set(attributes, merge=True) if upsert else doc_ref.update(
            attributes)
    if not batch:
        attributes = doc_ref.get().to_dict()
    return attributes, batch


def delete_document(collection_name, document_id, batch=None):
    attributes = dict(id=document_id, active=False)
    results = update_document(collection_name=collection_name,
                              document_id=document_id, attributes=attributes, batch=batch)
    return results


def iterate(collection_name, client, batch_size=1000, cursor=None, order_by_created_descending=False, filters: dict = None):
    query = client.collection(collection_name)
    if filters:
        for name, value in filters.items():
            query = query.where(name, '==', value)
    query = query.limit(batch_size)
    if order_by_created_descending:
        query = query.order_by('created', direction=firestore.Query.DESCENDING)
    if cursor:
        query = query.start_after(cursor)
    for doc in query.stream():
        yield doc
        if 'doc' in locals():
            yield from iterate(collection_name=collection_name, client=client, batch_size=batch_size, cursor=doc,
                               order_by_created_descending=order_by_created_descending, filters=filters)
