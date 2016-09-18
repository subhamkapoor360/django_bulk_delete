from django.db import connection

CHAIN_DICT = dict()

def get_accoring_to_db(db, value):
    if db == 'mysql':
        return value
    elif db == 'postgres':
        return '"' + value + '"'


def my_custom_sql(string):
    cursor = connection.cursor()
    cursor.execute(string)
    row = cursor.fetchall()
    return row


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

def delete_wrap(query_set):
    _model = query_set.model
    all_fields = [field for field in _model._meta.get_fields()
                  if field.one_to_many]
    string = "delete %s from %s %s where %s in %s"

    # _all_tables = [
    #     get_accoring_to_db(
    #         db_name,
    #         field.related_model._meta.db_table) for field in all_fields]
    _all_tables = [field.related_model for field in all_fields]
    main_table = get_accoring_to_db(db_name, _model._meta.db_table)
    middle_string = ""
    for field in all_fields:
        rel_model = field.related_model
        chain_models = levels_of_hierarchy(rel_model)
        # chain_models = refine_chain_dict(chain_models)
        _field_model = get_accoring_to_db(
            db_name, field.related_model._meta.db_table)
        # _field_model_pk = field.related_model._meta.pk.column
        _field_model_pk = get_accoring_to_db(
            db_name, field.get_joining_columns()[0][1])
        _tmp_str = 'INNER JOIN ' + _field_model + ' ON '
        _tmp_str = _tmp_str + _field_model + '.' + _field_model_pk + ' = ' + \
            main_table + '.' + \
                get_accoring_to_db(db_name, _model._meta.pk.column) + ' '
        middle_string += _tmp_str
        chain_str = list()
        if chain_models != None:
            for model_name, field_list in chain_models.iteritems():
                for rel_field in field_list:
                    chain_str.append('INNER JOIN ' + get_accoring_to_db(db_name, rel_field.related_model._meta.db_table) + " ON " + get_accoring_to_db(db_name, model_name._meta.db_table) +
                        "." + get_accoring_to_db(db_name, model_name._meta.pk.column) + ' = ' + get_accoring_to_db(db_name, rel_field.related_model._meta.db_table) + "." + get_accoring_to_db(db_name, rel_field.get_joining_columns()[0][1]) + " ")
                    # _all_tables.append(rel_field.related_model._meta.db_table)
                    _all_tables.append(rel_field.related_model)
    for joins in chain_str:
        middle_string += joins
    _all_tables = list(set(_all_tables))
    _all_tables.append(_model)
    _all_tables_tmp = []
    for t in _all_tables:
        _all_tables_tmp.append(
    t._meta.db_table +
    "." +
    t._meta.pk.column +
    " as " +
    t._meta.db_table +
    "_" +
     t._meta.pk.column)
    str_0 = ', '.join(list(set(_all_tables_tmp)))
    str_3 = main_table + "." + get_accoring_to_db(
        db_name, _model._meta.pk.column)
    query_set = query_set.values_list('pk')
    str_4, value_tuple = get_query_builder(query_set, connection)
    if 'WHERE' in str_4:
        str_5 = str_4.split('WHERE')[-1]
    else:
        str_5 = ''
    # string = "delete " + str_0 + " from " +main_table + " " + middle_string + " where " + str_3 + " in ("+ str_4 + ") "
    # query_str_1 = "delete " + str_0 + " from " + main_table + " " + \
    #     middle_string + " where " + str_3 + " in (" + str_4 + ") "
    query_str_1 = "select " + str_0 + " from " + main_table + " " + \
        middle_string + " where " + str_3 + " in (" + str_4 + ") "
    if str_5 != '':
        # query_str_2 = "delete " + main_table + " from " + main_table + "
        # where " + str_5
        query_str_2 = "select " + main_table + " from " + main_table + " where " + str_5
    else:
        # query_str_2 = "delete " + main_table + " from " + main_table
        query_str_2 = "select " + main_table + " from " + main_table
    tuples = my_custom_sql(query_str_1 % value_tuple)
    final_delete_query(_all_tables, tuples)

    return string


def final_delete_query(table_list, tuples):
    i = 0
    id_list = []
    while i <len(tuples[0]):
        tmp = []
        for v in tuples:
           tmp.append(v[i])
        id_list.append(tmp)
        i= i + 1
    k = 0
    sql_queries = []
    while k<len(table_list):
        id_params = ",".join([str(ii) for ii in id_list[k]])
        sql_queries.append("Delete from " + table_list[k]._meta.db_table + " WHERE " + table_list[k]._meta.pk.column + " IN (" + id_params +")")
        k+=1
    for i in sql_queries:
        my_custom_sql(i)





