import pandas as pd

oracle_operators = {
    '==': '=', '!=': '!=', 'LIKE': 'LIKE',
    'IN': 'IN', 'BETWEEN': 'BETWEEN', '>=': '>=',
    '<=': '<=', 'NOT IN': 'NOT IN', '>': '>', '<': '<'
}
sql_server_operators = {
    '==': '=', '!=': '!=', 'LIKE': 'LIKE',
    'IN': 'IN', 'BETWEEN': 'BETWEEN', '>=': '>=',
    '<=': '<=', 'NOT IN': 'NOT IN', '>': '>', '<': '<'
}


def get_operators(column):
    operators = {
        '==': [], '!=': [], 'LIKE': [],
        'IN': [], 'BETWEEN': [], '>=': [],
        '<=': [], 'NOT IN': [], '>': [], '<': []
    }
    # get the params
    for my_filter in column['filtros']:
        if int(my_filter['tipo_valor']) == 2:
            my_filter['valor_filtro'] = f"\'{my_filter['valor_filtro']}\'"
        operators[my_filter['operador']].append(my_filter['valor_filtro'])

    if len(operators['==']) > 1 or (len(operators['==']) >= 1 and len(operators['IN']) >= 1):
        operators['IN'] = operators['IN'] + operators['==']
        operators['=='] = []
    if len(operators['!=']) > 1 or (len(operators['!=']) >= 1 and len(operators['NOT IN']) >= 1):
        operators['NOT IN'] = operators['NOT IN'] + operators['!=']
        operators['!='] = []

    return operators


def get_where(columns, sql_source):
    where = ''
    current_operators = {}
    if sql_source == 1:  # SQL SERVER
        current_operators = sql_server_operators
    else:
        current_operators = oracle_operators

    for column in columns:
        operators = get_operators(column)
        for operator in operators:
            # IN - NOT IN
            if (operator == 'IN' and len(operators[operator]) > 0) or (
                    operator == 'NOT IN' and len(operators[operator]) > 0):
                in_field = f"{column['mapeo']} {current_operators[operator]} ("
                for value in operators[operator]:
                    in_field += str(value) + ', '
                in_field = in_field[:len(in_field) - 2] + ') AND '
                where += in_field
            # BETWEEN
            elif operator == 'BETWEEN':
                for value in operators[operator]:
                    val1, val2 = value[1:len(value) - 1].split('|')  # remove quotes and split
                    val1 = f'\'{val1}\''
                    val2 = f'\'{val2}\''
                    where += column['mapeo'] + ' ' + current_operators[operator] + ' ' + str(val1) + ' AND ' \
                             + str(val2) + ' AND '
            else:
                for value in operators[operator]:
                    where += column['mapeo'] + ' ' + current_operators[operator] + ' ' + str(value) + ' AND '
    where = where[:len(where) - 5]
    return where


def get_filter_query(columns, table='', sql_source=1):

    if sql_source ==1:

        query = 'SELECT '
        if len(columns) == 0:
            query += '*  '
        for column in columns:
            query += column['mapeo'] + ', '
        query = query[:len(query) - 2] + f",  ROW_NUMBER() OVER(ORDER BY {columns[0]['mapeo']}) AS indice FROM {table} WHERE "
        query += get_where(columns, sql_source)
        if query[len(query) - 6:] == 'WHERE ':  # check no filters
            query = query[:len(query) - 6]


    if sql_source == 2:

        query = 'SELECT '
        if len(columns) == 0:
            query += '*  '
        for column in columns:
            query += column['mapeo'] + ', '
        query = query[:len(query) - 2] + f', ROWNUM indice FROM {table} WHERE '
        query += get_where(columns, sql_source)
        if query[len(query) - 6:] == 'WHERE ':  # check no filters
            query = query[:len(query) - 6]
    return query


def apply_filter_to_dataframe(dataframe, columns):
    no_arithmetic = {'LIKE': like_to_pandas, 'IN': in_to_pandas, 'BETWEEN': between_to_pandas,
                     'NOT IN': not_in_to_pandas}
    filters_to_process = {}
    for column in columns:
        filters_to_process[column['mapeo']] = get_operators(column)
    # First apply arithmetic operators
    dataframe = arithmetic_to_pandas(dataframe, filters_to_process)
    for operator in no_arithmetic:
        if operator == 'BETWEEN':
            dataframe = no_arithmetic[operator](dataframe, filters_to_process, columns)
        else:
            dataframe = no_arithmetic[operator](dataframe, filters_to_process)

    if dataframe.shape[0] == 0:
        raise ("error filtro")
    return dataframe


def arithmetic_to_pandas(dataframe, values):
    not_arithmetic = ['LIKE', 'IN', 'BETWEEN', 'NOT IN']
    for value in values:
        for operator in values[value]:
            if operator not in not_arithmetic:
                for filter_value in values[value][operator]:
                    dataframe = dataframe.query(f"{value} {operator} {filter_value} ")
    return dataframe


def like_to_pandas(dataframe, values):
    for value in values:
        for filter_value in values[value]['LIKE']:
            dataframe = dataframe.query(f"{value}.str.contains({filter_value})", engine='python')
    return dataframe


def in_to_pandas(dataframe, values):
    for value in values:
        # pass
        if len(values[value]['IN']) > 0:
            dataframe = dataframe[dataframe[value].isin(values[value]['IN'])]
    return dataframe


def not_in_to_pandas(dataframe, values):
    for value in values:
        # pass
        if len(values[value]['NOT IN']) > 0:
            dataframe = dataframe[~dataframe[value].isin(values[value]['NOT IN'])]
    return dataframe


def between_to_pandas(dataframe, values, data):
    for value in values:
        if len(values[value]['BETWEEN']) > 0:
            my_type = 1
            for dat in data:
                if dat['mapeo'] == value:
                    my_type = dat['tipo']
            val1, val2 = values[value]['BETWEEN'][0].split('|')  # remove quotes and split
            val1, val2 = val1[1:], val2[:len(val2) - 1]
            # check types

            if my_type == 1:
                try:
                    val1, val2 = int(val1), int(val2)
                except:

                    return dataframe
            elif my_type == 4:
                try:
                    val1, val2 = float(val1), float(val2)
                except:

                    return dataframe
            dataframe = dataframe[(dataframe[value] >= val1) & (dataframe[value] <= val2)]

    return dataframe


if __name__ == '__main__':
    cols = [
        {
            "mapeo": "ID_ANIOMES",
            "tipo": 2,
            "filtros":
                [
                    {
                        "operador": "IN",
                        "tipo_valor": 1,
                        "valor_filtro": "201508"
                    },
                    {
                        "operador": "BETWEEN",
                        "tipo_valor": 2,
                        "valor_filtro": "201508|201509"
                    }

                ]
        },
        {
            "mapeo": "ID_CLIPADRE",
            "tipo": 1,
            "filtros":
                [
                    {
                        "operador": "<=",
                        "tipo_valor": 1,
                        "valor_filtro": "9276"
                    }
                ]
        },
        {
            "mapeo": "ID_MARCA_PADRE",
            "tipo": 2,
            "filtros":
                [
                ]
        },
        {
            "mapeo": "INDICE",
            "tipo": 2,
            "filtros":
                [
                    {
                        "operador": "!=",
                        "tipo_valor": 1,
                        "valor_filtro": "6000002"
                    }
                ]
        },
        {
            "mapeo": "ID_SUBMERCADO",
            "tipo": 2,
            "filtros":
                [
                    {
                        "operador": "LIKE",
                        "tipo_valor": 2,
                        "valor_filtro": "07R"
                    }
                ]
        }
    ]

    columns = []
    for col in cols:
        columns.append(col['mapeo'])

    file = pd.read_csv('../../media/1/input/resultado104.csv', usecols=columns)

    apply_filter_to_dataframe(file, cols)
