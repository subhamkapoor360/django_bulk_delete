from django.db import connection

CHAIN_DICT = dict()


def get_mapping(from_model, to_model, next_queue=set([]), visited=[]):
    lss = []
    visited.append(from_model)
    if from_model is None or to_model is None:
        return
    fm_fields = from_model._meta.get_all_related_objects()
    for f in fm_fields:
        if not (fieldmany_to_one or fieldmany_to_many):
            continue
        #import ipdb; ipdb.set_trace()
        if (fieldmany_to_one or fieldmany_to_many) and fieldrelated_model == to_model:
            return field.name
        if fieldrelated_model not in visited:
            next_queue.add(fieldrelated_model)
    print fieldrelated_model
    if len(next_queue) > 0:
        get_mapping(next_queue.pop(), to_model, visited_models)
    return None


def get_accoring_to_db(db, value):
    if db == 'mysql':
        return value
    elif db == 'postgres':
        return '"' + value + '"'


def my_custom_sql(string):
    cursor = connection.cursor()
    cursor.execute(string)
    # row = cursor.fetchone()
    # return row


def get_query_builder(query_set, connection):
    compiler = query_set.query.get_compiler(connection=connection)
    sql_tuple = compiler.as_sql()
    value_tuple = sql_tuple[-1]
    value_tuple = tuple(
        ['"' + v + '"' if type(v) in [str, unicode] else v for v in value_tuple])
    print value_tuple
    sql_tuple = [v for v in sql_tuple]
    sql_tuple[-1] = value_tuple
    query_string = sql_tuple[0]
    values = sql_tuple[-1]
    return query_string, value_tuple


def levels_of_hierarchy(rel_model):
    all_fields = [field for field in rel_model._meta.get_fields()
                  if field.one_to_many]
    for field in all_fields:
        rltd_model = field.related_model
        if rel_model in CHAIN_DICT:
            CHAIN_DICT[rel_model].append(field)
        else:
            CHAIN_DICT[rel_model] = [field]
        foreign_fields = [
            field for field in rltd_model._meta.get_fields() if field.one_to_many]
        if len(foreign_fields) != 0:
            levels_of_hierarchy(rltd_model)
        else:
            return CHAIN_DICT

# def refine_chain_dict(chain_dict):
#     check_list = []
#     check_dict = dict()
#     for table,field_list in chain_dict.iteritems():
#         second_check = []
#         for field in field_list:
#             field_class = field.related_model
#             if field_class not in check_list:
#                 second_check.append(field)
#                 check_list.append(field_class)
#             if table not in check_dict:
#                 check_dict[table] = second_check
#     print check_list
#     return check_dict





def delete_wrap(query_set, cascade_true=True, db_name='postgres'):
    _model = query_set.model
    all_fields = [field for field in _model._meta.get_fields()
                  if field.one_to_many]
    string = "delete %s from %s %s where %s in %s"

    _all_tables = [
        get_accoring_to_db(
            db_name,
            field.related_model._meta.db_table) for field in all_fields]
    main_table = get_accoring_to_db(db_name, _model._meta.db_table)
    middle_string = ""
    for field in all_fields:
        rel_model = field.related_model
        chain_models = levels_of_hierarchy(rel_model)
        #chain_models = refine_chain_dict(chain_models)
        _field_model = get_accoring_to_db(
            db_name, field.related_model._meta.db_table)
        # _field_model_pk = field.related_model._meta.pk.column
        _field_model_pk = get_accoring_to_db(
            db_name, field.get_joining_columns()[0][1])
        _tmp_str = 'INNER JOIN ' + _field_model + ' ON '
        _tmp_str = _tmp_str + _field_model + '.' + _field_model_pk + ' = ' + \
            main_table + '.' + get_accoring_to_db(db_name, _model._meta.pk.column) + ' '
        middle_string += _tmp_str
        chain_str = list()
    for model_name, field_list in chain_models.iteritems():
        for rel_field in field_list:
            chain_str.append('INNER JOIN ' + rel_field.related_model._meta.db_table + " ON " + model_name._meta.db_table + \
                "." + model_name._meta.pk.column + ' = ' + rel_field.related_model._meta.db_table + "." + rel_field.get_joining_columns()[0][1] + " ")
            _all_tables.append(rel_field.related_model._meta.db_table)
    for joins in chain_str:
        middle_string += joins
    str_0 = ', '.join(list(set(_all_tables)))
    str_3 = get_accoring_to_db(
        db_name,
        main_table +
        "." +
        _model._meta.pk.column)
    query_set = query_set.values_list('pk')
    str_4, value_tuple = get_query_builder(query_set, connection)
    if 'WHERE' in str_4:
        str_5 = str_4.split('WHERE')[-1]
    else:
        str_5 = ''
    # string = "delete " + str_0 + " from " +main_table + " " + middle_string + " where " + str_3 + " in ("+ str_4 + ") "
    query_str_1 = "delete " + str_0 + " from " + main_table + " " + \
        middle_string + " where " + str_3 + " in (" + str_4 + ") "
    if str_5 != '':
        query_str_2 = "delete " + main_table + " from " + main_table + " where " + str_5
    else:
        query_str_2 = "delete " + main_table + " from " + main_table
    #my_custom_sql(query_str_1 % value_tuple)
    #my_custom_sql(query_str_2 % value_tuple)
    print query_str_2 % value_tuple
    print query_str_1 % value_tuple
    return string