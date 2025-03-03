import json 
import psycopg2
import sqlparse
import os
import re
import subprocess
import ast
import time
from collections.abc import Iterable
from multiprocessing import Process, Manager, Semaphore, Lock

from functions import execute_sql_view
from config import DBConfig

import logging

# from datetime import datetime, timezone, timedelta

logger = logging.getLogger('log')
root_dir = os.path.dirname(os.path.abspath(__file__))

parsing_time = 0

sql_keywords = [
    'add', 'all', 'alter', 'and', 'any', 'as', 'asc', 'autoincrement', 'between', 'boolean', 
    'by', 'call', 'case', 'cast', 'char', 'column', 'commit', 'constraint', 'create', 'cross', 
    'current_date', 'current_time', 'current_timestamp', 'database', 'date', 'default', 
    'delete', 'desc', 'distinct', 'drop', 'else', 'end', 'exists', 'extract', 'false', 
    'foreign', 'from', 'full', 'function', 'grant', 'group', 'having', 'if', 'in', 
    'inner', 'insert', 'int', 'integer', 'intersect', 'into', 'is', 'join', 'key', 
    'left', 'like', 'limit', 'not', 'null', 'on', 'or', 'order', 'outer', 'primary', 
    'procedure', 'rename', 'right', 'rollback', 'row', 'select', 'set', 'show', 
    'table', 'then', 'to', 'truncate', 'union', 'update', 'values', 'view', 'where', 
    'with', 'true', 'unique', 'alter', 'table', 'index', 'view', 'user', 'load', 
    'replace', 'insert', 'returning', 'group_concat', 'extract', 'recursive', 'isnull'
]
sql_functions = [
    "sum", "count", "avg", "max", "min", "extract", "group_concat", "string_agg", "variance", "stddev",
    "median", "percentile_cont", "percentile_disc", "abs", "ceiling", "ceil", "floor", "round", "power", 
    "exp", "extract", "log", "sqrt", "sin", "cos", "tan", "concat", "substring", "substr", "upper", "lower",
    "trim", "length", "replace", "lpad", "rpad", "now", "date_add", "date_sub",
    "date_part", "to_date", "to_char", "coalesce", "nullif", "case", "greatest", "least",
    "json_extract", "json_agg", "xmlagg", "st_distance", "st_intersects", "st_union",
    "row_number", "rank", "dense_rank", "lead", "lag", "ntile", "cast"
]
sql_constants = ["current_date", "current_time", "current_timestamp", "localtime",
    "localtimestamp", "session_user", "current_user", "system_user", "user", "null"
]

identifier_pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
table_dot_column = r'\b[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b'
constant_pattern = r"('(?:''|[^'])*')|(\b\d+\b)|(\b\d+\.\d+\b)|(\bTRUE\b|\bFALSE\b|\bNULL\b)"

def extract_number(key):
    match = re.search(r'\d+', key)
    return int(match.group()) if match else float('inf')

def execute_sql(sql, db_name, schema = 'public'):
    conn = psycopg2.connect(
        host = DBConfig.host,
        user = DBConfig.user,
        password = DBConfig.password,
        port = DBConfig.port,
        dbname = db_name
    )
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    conn.commit()
    cursor.close()

    results = None
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout TO 360000;")
        cur.execute(sql)
        results = cur.fetchall()
        # print(results)
    except psycopg2.Error as e:
        # logger.error(f"An error occurred in SQL {sql} : {e}")
        results = []
    finally:
        cur.close()
        conn.close()
    
    return results

def get_table_names_from_db(db_name, schema = 'public'):
    sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';"
    results = execute_sql(sql, db_name, schema)

    if results is not None:
        table_names = [res[0] for res in results]
    else:
        table_names = []
    
    return table_names

def get_table_columns_from_db(db_name, schema = 'public'):
    sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}';"
    results = execute_sql(sql, db_name, schema)

    if results is not None:
        table_names = [res[0] for res in results]
    else:
        table_names = []
     
    table_columns = {}   
    if table_names != [] :
        for table in table_names :
            sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';"
            results = execute_sql(sql, db_name, schema)
            if results is not None:
                columns = [row[0] for row in results]
            else :
                columns = []
            table_columns[table] = columns
    
    return table_columns

def get_db_info(db_name, table_names, schema = 'public'):
    # get information each table (including CREATE TABLE, FOREIGN KEY, PRIMARY KEY, etc.)
    create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements = [], [], [], []
    for table_name in table_names:
        os.environ['PGPASSWORD'] = DBConfig.password
        command = 'pg_dump -s -h {} -p {} -U {} -d {} -t "{}.{}" --schema-only'.format(
            DBConfig.host,
            DBConfig.port,
            DBConfig.user,
            db_name,
            schema,
            table_name
        )
        # print(command)

        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        output, error = process.communicate()

        if error is None:
            database_dump = output.decode()
            filtered_lines = [line for line in database_dump.split("\n") 
                if not (line.strip() == "" or line.startswith("SET") or line.startswith("--") or line.startswith("SELECT") or line.startswith("pg_dump"))]
            database_dump = "\n".join(filtered_lines)
           
            statements = database_dump.split(";")
            for stmt in statements:
                if stmt.upper().startswith("CREATE TABLE"):
                    create_table_statements.append(stmt.strip().replace(f"{schema}.", "") + ";")
                elif "PRIMARY KEY" in stmt.upper():
                    primary_key_constraints.append(stmt.strip().replace(f"{schema}.", "") + ";")
                elif "FOREIGN KEY" in stmt.upper():
                    foreign_key_constraints.append(stmt.strip().replace(f"{schema}.", "") + ";")
        else:
            print(f"Error getting DDL for table {table_name}: {error}")

        get_indexes_sql = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{}';".format(table_name)
        for res in execute_sql(get_indexes_sql, db_name, schema):
            create_index_statements.append(res[1].replace("public.", ""))

    create_table_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_table_statements]
    primary_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in primary_key_constraints]
    foreign_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in foreign_key_constraints]
    create_index_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_index_statements]

    return create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements

def get_db_schema(db_name, path, schema = 'public'):
    table_names = get_table_names_from_db(db_name, schema)
    create_table_statements, primary_key_statements, foreign_key_statements, create_index_statements = get_db_info(db_name, table_names, schema)
    
    db_schema = dict()
    data_types = []
    table_info_list = []
    for table_name in table_names:
        table_info = dict()
        table_info["table"] = table_name
        rows = execute_sql(f"SELECT COUNT(*) FROM {table_name};", db_name, schema)[0][0]
        table_info["rows"] = rows
        table_info["columns"] = []
        columns_and_types = execute_sql(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';", db_name, schema)
        for (column, data_type) in columns_and_types:
            data_type = data_type.upper()
            
            table_info["columns"].append(
                {
                    "name": column,
                    "type": data_type
                }
            )
            data_types.append(data_type)
        
        table_ddl = None
        for ddl in create_table_statements:
            if "CREATE TABLE {} (".format(table_name).lower() in ddl.lower():
                table_ddl = ddl
                break
        
        if table_ddl is None:
            logger.warning(f"Can not find ddl for table {table_name}")
            # raise ValueError("can not find ddl for table {}".format(table_name))

        table_info["ddl"] = table_ddl

        table_info_list.append(table_info)

    db_schema["table_info"] = table_info_list
    db_schema["primary_key_statements"] = primary_key_statements
    db_schema["foreign_key_statements"] = foreign_key_statements
    db_schema["create_index_statements"] = create_index_statements

    # f"./data/{db_name}_schema.json"
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(db_schema, indent=2, ensure_ascii=False))

def schema_pruning(db_name, sql, path):
    with open(path, 'r') as file :
        schema = json.load(file)
    sql = normalize_sql(sql)
    sql = sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower')
    sql_tokens = sql.split()
    sql_tokens = [token.replace('\"', '').lower() for token in sql_tokens]
    # print(sql_tokens)
    
    used_table_names = []
    used_column_info = []
    used_table_ddls = []
    used_column_info_rows = []
    
    for table in schema["table_info"]:
        if table["table"] in sql_tokens:
            used_table_names.append(table["table"])
            used_table_ddls.append(table["ddl"])
            for column in table["columns"]:
                if any(column["name"] in sql_token for sql_token in sql_tokens):
                    used_column_info.append(table["table"] + "." + column["name"] + ": " + column["type"])
                    used_column_info_rows.append((table["table"] + "." + column["name"] + ": " + column["type"], table['rows']))
    
    used_pk_stmts, used_fk_stmts, used_index_stmts = [], [], []
    
    for primary_key_statement in schema["primary_key_statements"]:
        for table_name in used_table_names:
            if "ALTER TABLE ONLY {}\n".format(table_name).lower() in primary_key_statement.lower() and primary_key_statement not in used_pk_stmts:
                used_pk_stmts.append(primary_key_statement)

    for foreign_key_statement in schema["foreign_key_statements"]:
        fk_source_table_name = foreign_key_statement.replace("ALTER TABLE ONLY ", "").split("\n")[0].strip()
        fk_target_table_name = foreign_key_statement.split("REFERENCES")[1].split("(")[0].strip()
        if fk_source_table_name.lower() in used_table_names and fk_target_table_name.lower() in used_table_names and foreign_key_statement not in used_fk_stmts:
            used_fk_stmts.append(foreign_key_statement)
    
    for create_index_statement in schema["create_index_statements"]:
        for table_name in used_table_names:
            if "ON {} USING".format(table_name).lower() in create_index_statement.lower() and create_index_statement not in used_index_stmts:
                used_index_stmts.append(create_index_statement)
    
    # execution_results = execute_sql("EXPLAIN {}".format(sql), db_name)
    # query_plan = "\n".join([row[0] for row in execution_results])
    # print(type(query_plan))

    pk = "\n".join(used_pk_stmts) if len(used_pk_stmts) != 0 else "None"
    fk = "\n".join(used_fk_stmts) if len(used_fk_stmts) != 0 else "None"
    indexes = "\n".join(used_index_stmts) if len(used_index_stmts) != 0 else "None"

    return used_column_info, used_column_info_rows, used_table_ddls, pk, fk, indexes

def get_tables_columns_names(schema_path) :
    tables_columns = {}
    with open(schema_path, 'r') as f :
        schema = json.load(f)
    for table in schema['table_info'] :
        tables_columns[table['table']] = [col_info['name'] for col_info in table['columns']]
    return tables_columns

def get_tables_names(schema_path) :
    tables = []
    with open(schema_path, 'r') as f :
        schema = json.load(f)
    for table in schema['table_info'] :
        tables.append(table["table"])
    return tables

def find_view_info(db_name, schema = 'public') :
    views = f"""
    SELECT viewname
    FROM pg_views
    WHERE schemaname = '{schema}';
    """
    views = execute_sql(views, db_name, schema)
    views_columns = []
    views2columns = {}
    
    for view in views :
        view_cols = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{view[0]}';
        """
        views_columns_ = [row[0] for row in execute_sql(view_cols, db_name, schema)]
        views_columns += views_columns_
        views2columns[view[0]] = views_columns_
        
    return views2columns, views_columns
 
## ndv of all tables 
def get_ndvs_all(conn, ndv_path, schema = 'public') :
    ndvs = {}
    try :
        cur = conn.cursor()
        cur.execute("analyze;")
        # get tables
        sql = f"""SELECT tablename
            FROM pg_tables
            WHERE schemaname = '{schema}';
            """
        cur.execute(sql)
        tables = cur.fetchall()
        # print(tables)
        for table in tables :
            table = table[0]
            sql = f"""
                WITH total_rows AS (
                    SELECT COUNT(*) AS total FROM {table}
                )
                SELECT 
                    attname, 
                    CASE 
                        WHEN n_distinct >= 0 THEN 
                            (n_distinct / NULLIF(total_rows.total, 0)) -- 避免除以0
                        ELSE 
                            (n_distinct * -1) -- 保留负数部分，表示百分比
                    END AS n_distinct_percentage
                FROM 
                    pg_stats, 
                    total_rows
                WHERE 
                    tablename = '{table}'
                    AND
                    schemaname = '{schema}';
                """
            cur.execute(sql)
            res = cur.fetchall()
            for r in res :
                ndvs[f"{table}.{r[0]}"] = r[1]

            sql = f"select count(*) from {schema}.{table};"
            cur.execute(sql)
            total_rows = cur.fetchone()[0]

            if total_rows == 0 :
                sql = f"select column_name from information_schema.columns where table_name = '{table}';"
                cur.execute(sql)
                results = cur.fetchall()
                for row in results :
                    ndvs[f"{table}.{row[0]}"] = 0
    except Exception as e :
        print(f"Error: {e} --> {sql}")
        exit()
    finally :
        if cur :
            cur.close()
    
    with open(ndv_path, 'w') as f:
        json.dump(ndvs, f, indent=4)
        
    return ndvs
     
## selectivity
def normalize_tokenlist(tokenlist) :
    normalized_tl = ""
    for token in tokenlist :
        if token.ttype is not None :
            if '-' in token.value : 
                if token.ttype == sqlparse.tokens.Token.Operator :
                    tmp_values = token.value.split('-')
                    normalized_tl += ' - '.join(tmp for tmp in tmp_values)
                elif token.ttype == sqlparse.tokens.Token.Literal.String.Single :
                    normalized_tl += token.value + " "
            else : normalized_tl += token.value + " "
        else :
            if any(t is not None for t in token) :
                normalized_tl += normalize_tokenlist(token) + " "
            else :
                normalized_tl += token.value + " "
            
    return normalized_tl

def normalize_sql(sql) :
    parsed_sql = sqlparse.parse(sql)
    normalized_sql = ""
    
    for psql in parsed_sql :
        for token in psql :
            # print(token, token.__class__, token.ttype)
            if token.ttype is not None :
                normalized_sql += token.value + " "
            else :
                normalized_sql += normalize_tokenlist(token) + " "
    # .replace(" - ", "-") --> there might be error in the subtraction operation
    normalized_sql = re.sub(r'\s+', ' ', normalized_sql).strip().replace(" . ", ".") 
    
    return normalized_sql

def is_subquery(token_list):
    if isinstance(token_list, sqlparse.sql.TokenList):
        for token in token_list.tokens:
            if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                return True
    return False

def extract_subqueries(token_list):
    subqueries = []
    if is_subquery(token_list):
        subqueries.append(token_list)
    if isinstance(token_list, Iterable) :
        for token in token_list.tokens:
            if isinstance(token, sqlparse.sql.TokenList):
                subqueries.extend(extract_subqueries(token))
    return subqueries

def is_literal_list(string):
    try:
        result = ast.literal_eval(string)            
        return isinstance(result, list)
    except (ValueError, SyntaxError):
        if ',' in string and all(sql_keyword not in string for sql_keyword in sql_keywords) and all(sql_function not in string for sql_function in sql_functions) :
            return True
        else :
            return False
    
def extract_alias(sql, conn, tables_columns) :    
    alias = {} ## {alias_name: [(table, column)]}
    simple_alias = {}
    complex_alias = {}
    
    ## get table names
    table_names = list(tables_columns.keys())
    # print(table_names)
    column_names = []
    for columns in tables_columns :
        column_names.extend(columns)    
    
    parsed_sql = sqlparse.parse(normalize_sql(sql))
    if len(parsed_sql) == 1 :        
        parsed_sql = parsed_sql[0]
        aggregation = ""
        with_subquery = False
        keep_identifier = False
        
        ## extract alias pairs from sql [table, columns, expressions, subquery]
        for token in parsed_sql.tokens :
            # print(token, token.__class__, token.ttype)
            if isinstance(token, sqlparse.sql.IdentifierList) :
                if with_subquery :
                    for identifier in token.get_identifiers() :
                        if " as " in identifier.value.lower() : 
                            if " as " in identifier.value :
                                al = token.value.split(" as ")[0]
                                rl = token.value.replace(al + " as ", '').strip()
                            else :
                                al = token.value.split(" AS ")[0]
                                rl = token.value.replace(al + " AS ", '').strip()
                        pair = (rl, al)
                        
                        # extract_alias for subquery
                        subsql = rl[1:-1].strip()
                        s_sa, c_sa = extract_alias(subsql, conn, tables_columns)
                        simple_alias.update(s_sa)
                        complex_alias.update(c_sa)
                        
                        if pair != [] and pair[1].lower() not in sql_keywords: alias[pair[1]] = pair[0]
                    with_subquery = False
                else :
                    for identifier in token.get_identifiers() :
                        # print(identifier, identifier.__class__, identifier.ttype)
                        if isinstance(identifier, sqlparse.sql.Identifier) :
                            if identifier.get_alias() : # table/column as alias | table/column alias
                                al = identifier.value.split()[-1]
                                rl = ' '.join(identifier.value.split()[:-1])
                                if rl[-2:].lower() == 'as' : rl = rl[:-2].strip()
                            elif identifier.value in table_names or identifier.value in column_names :
                                keep_identifier = True
                                rl = identifier.value 
                                continue
                            else : continue    
                            pair = (rl, al)
                            if rl.startswith('(') and rl.endswith(')') and 'select' in rl.lower().split() : # subquery
                                subsql = rl[1:-1].strip()
                                s_sa, c_sa = extract_alias(subsql, conn, tables_columns)
                                simple_alias.update(s_sa)
                                complex_alias.update(c_sa)
                            if aggregation != "" :
                                pair = (aggregation + pair[0], pair[1])
                                aggregation = ""
                            if pair != [] and pair[1].lower() not in sql_keywords: # case that desc as pair[0]
                                alias[pair[1]] = pair[0]
                        elif isinstance(identifier, sqlparse.sql.Token) : # function
                            if keep_identifier and identifier.value.strip() != "" :
                                if parsed_sql.tokens.index(token) + 1 > len(parsed_sql.tokens) or (parsed_sql.tokens.index(token) + 2 < len(parsed_sql.tokens) and parsed_sql.tokens[parsed_sql.tokens.index(token) + 1].value != '(' and parsed_sql.tokens[parsed_sql.tokens.index(token) + 2].value.lower() in sql_keywords) :
                                    al = identifier.value
                                    pair = (rl, al)
                                    if aggregation != "" :
                                        pair = (aggregation + pair[0], pair[1])
                                        aggregation = ""
                                    if pair != [] and pair[1].lower() not in sql_keywords: # case that desc as pair[0]
                                        alias[pair[1]] = pair[0]
                                elif identifier.value.lower() in sql_functions :
                                    aggregation = identifier.value 
                                
                                keep_identifier = False
                            elif identifier.value.lower() in sql_functions :
                                aggregation = identifier.value 
            elif isinstance(token, sqlparse.sql.Identifier) :
                if with_subquery : # with (subquery) as alias
                    if " as " in token.value.lower() : 
                        if " as " in token.value :  
                            al = token.value.split(" as ")[0]
                            rl = token.value.replace(al + " as ", '').strip()
                        else :
                            al = token.value.split(" AS ")[0]
                            rl = token.value.replace(al + " AS ", '').strip()
                    with_subquery = False
                elif token.get_alias() : # table/column as alias | table/column alias
                    al = token.value.split()[-1]
                    rl = ' '.join(token.value.split()[:-1])
                    if rl[-2:].lower() == 'as' : rl = rl[:-2].strip()
                elif token.value in table_names or token.value in column_names :
                    keep_identifier = True
                    rl = token.value
                    continue
                else : continue
                pair = (rl, al)
                if rl.startswith('(') and rl.endswith(')') and 'select' in rl.lower().split() : # subquery
                    subsql = rl[1:-1].strip()
                    s_sa, c_sa = extract_alias(subsql, conn, tables_columns)
                    simple_alias.update(s_sa)
                    complex_alias.update(c_sa)
                if aggregation != "" :
                    pair = (aggregation + pair[0], pair[1])
                    aggregation = ""
                if pair != [] and pair[1].lower() not in sql_keywords: # case that desc as pair[0]
                    alias[pair[1]] = pair[0]
            elif isinstance(token, sqlparse.sql.Token) :
                if token.value.lower() == "with" : # with subquery
                    with_subquery = True
                elif token.value.lower() in sql_functions :
                    aggregation = token.value
                elif aggregation != "" and token.value.lower() in sql_keywords and token.value.lower() not in sql_functions:
                    aggregation = ""
                elif isinstance(token, sqlparse.sql.Where) : # extract_alias for subquery in where
                    if '(' in token.value and ')' in token.value :
                        for t in token :
                            if t.value.startswith('(') and t.value.endswith(')') and 'select' in t.value.lower().split() : # subquery
                                subsql = t.value[1:-1].strip()
                                s_sa, c_sa = extract_alias(subsql, conn, tables_columns)
                                simple_alias.update(s_sa)
                                complex_alias.update(c_sa)
                else : 
                    if keep_identifier and token.value.strip() != '' :
                        if parsed_sql.tokens.index(token) + 1 > len(parsed_sql.tokens) or (parsed_sql.tokens.index(token) + 2 < len(parsed_sql.tokens) and parsed_sql.tokens[parsed_sql.tokens.index(token) + 2].ttype is sqlparse.tokens.Keyword ) :
                            al = token.value
                            pair = (rl, al)
                            if aggregation != "" :
                                pair = (aggregation + pair[0], pair[1])
                                aggregation = ""
                            if pair != [] and pair[1].lower() not in sql_keywords: # case that desc as pair[0]
                                alias[pair[1]] = pair[0]
                        keep_identifier = False
        
        ## get single table or column alias [alias_name, real_name]
        single_alias = {}
        for alias_ in alias.items() :
            if re.fullmatch(identifier_pattern, alias_[1]) : # table or column
                single_alias[alias_[0]] = alias_[1]
        
        ## replace alias in table.column [without considering subquery or expression replacement][table + column / expression + subquery]
        for alias_ in alias.items() :
            if alias_[0] not in single_alias.keys() : 
                if re.fullmatch(table_dot_column, alias_[1]) : # table.column format
                    table = alias_[1].split(".")[0]
                    column = alias_[1].split(".")[1]
                    if table in single_alias.keys() : table = single_alias[table]
                    alias[alias_[0]] = (table, column)
                else : # other format with expression or subquery
                    tokens = alias_[1].split()
                    expression = ""
                    for token in tokens :
                        t = ''
                        if '.' in token : # table.column
                            table_name = token.split('.')[0]
                            if table_name in single_alias.keys() : 
                                table_name = single_alias[table_name]
                            col_name = token.split('.')[1]
                            if col_name in single_alias.keys() :
                                col_name = single_alias[col_name]
                            t = f'{table_name}.{col_name}'
                        else : # table/column
                            if token in single_alias.keys() :
                                t = single_alias[token]
                            else : t = token
                        expression += t + " "
                    alias[alias_[0]] = expression
            else : # table or column
                if alias_[1] in table_names : alias[alias_[0]] = (alias_[1],)
                else : # column name
                    find_column = False
                    for table in table_names :
                        columns = tables_columns[table]
                        if alias_[1] in columns :
                            alias[alias_[0]] = (table, alias_[1])
                            find_column = True
                            break
                    if not find_column : 
                        logger.debug(f"Warning [{alias_}] : Name {alias_[1]} not found in the tables")
                        alias[alias_[0]] = ('', alias_[1])
            
            if type(alias[alias_[0]]) is tuple :
                simple_alias[alias_[0]] = alias[alias_[0]]
            else :
                complex_alias[alias_[0]] = alias[alias_[0]]
    
    else :
        print("Error: SQL statement has more than one statement")
        exit()
    
    return simple_alias, complex_alias

def judge_identifier(tokens, db_name, table_columns, alias, schema = 'public') :
    ## get table names
    table_names = list(table_columns.keys())
    # print(table_names)
    column_names = []
    for columns in table_columns.values() :
        column_names.extend(columns)   
    
    view_info, view_cols = find_view_info(db_name, schema)
    view_names = list(view_info.keys())
    all_identifiers = column_names + table_names + list(alias.keys()) + view_cols + view_names
    
    #  res --> all_identifiers
    for token in tokens.value.split() :
        res = re.fullmatch(identifier_pattern, token)
        if res :
            if res[0] in all_identifiers : return True
        else :
            if re.fullmatch(table_dot_column, token) : return True
       
    if "( * )" in tokens.value : return True
    
    return False

def get_predicate_str(tokens, aggregation_func, simple_alias, db_name, used_tables, table_columns, alias, schema = 'public') :
    operator = ''
    predicate = ''
    where_condition = True
    cnt = 0
    
    ## left, operator, right
    for t in tokens :
        if t.ttype == sqlparse.tokens.Token.Operator.Comparison or (t.ttype == sqlparse.tokens.Token.Keyword and t.value.lower() in ["like", "in", "exists", "between"]):
            operator = t
            break
    
    # print(tokens.left, tokens.right)
    if 'select' in tokens.left.value.lower() or 'select' in tokens.right.value.lower() : return "", False
    
    if judge_identifier(tokens.left, db_name, table_columns, alias, schema) : cnt += 1
    if judge_identifier(tokens.right, db_name, table_columns, alias, schema): cnt += 1
    
    if cnt == 2 : where_condition = False # other predicates
    elif cnt == 1 :where_condition = True
    else : 
        return tokens.value, False

    left = rewrite_expression(tokens.left, simple_alias, used_tables, table_columns)
    right = rewrite_expression(tokens.right, simple_alias, used_tables, table_columns)
    
    ## left, operator, right [aggregation]
    predicate = aggregation_func + left + " " + operator.value + " " + right
    
    return predicate, where_condition

def get_preidcate(original_predicate, simple_alias, used_tables, table_columns) :
    rewrite_predicate = ""
    variables = original_predicate.split()
    for variable in variables :
        tmp = variable
        if '.' in variable : # table.column
            tab = variable.split('.')[0]
            col = variable.split('.')[1]
            if col in simple_alias.keys() :
                if len(simple_alias[col]) == 2 : tmp = f"{simple_alias[col][0]}.{simple_alias[col][1]}"
                else : 
                    if tab in simple_alias.keys() :
                        tmp = f'{simple_alias[tab][0]}.{col}'
            elif tab in simple_alias.keys() :
                tmp = f'{simple_alias[tab][0]}.{col}'
        else :
            if variable in simple_alias.keys() :
                if len(simple_alias[variable]) == 1 : tmp = simple_alias[variable][0]
                else : 
                    tmp = simple_alias[variable][0] + '.' + simple_alias[variable][1]
            else : # column name
                find = False
                for table in used_tables : 
                    if variable in table_columns[table] : 
                        tmp = table + '.' + variable
                        find = True
                        break
                if not find :
                    for table in table_columns.keys() :
                        if variable in table_columns[table] :
                            tmp = table + '.' + variable
                            find = True
                            break
        rewrite_predicate += tmp + " "    
        
    return rewrite_predicate   

def rewrite_expression(tokenlist, simple_alias, used_tables, table_columns) :
    original_predicate = tokenlist.value
    rewrite_predicate = get_preidcate(original_predicate, simple_alias, used_tables, table_columns)
    
    return rewrite_predicate

def find_table_info(db_name, schema_path, predicate, schema = 'public') :
    tables = []
    
    table_columns = get_tables_columns_names(schema_path)
    table_names = list(table_columns.keys())
    
    # print(schema) # table --> columns --> name
    items = predicate.split(' ')
    for item in items :
        if re.fullmatch(table_dot_column, item) : # table.column
            table = item.split('.')[0]
            if table in table_names and table not in tables : tables.append(table)
        elif re.fullmatch(identifier_pattern, item) : # column
            find = False
            for table, cols in table_columns.items() :
                for col in cols :
                    if col == item and table not in tables:
                        tables.append(table)
                        find = True
                        break
            if not find :
                view2columns, _ = find_view_info(db_name, schema)
                for v2c in view2columns.items():
                    if item in v2c[1] :
                        tables.append(v2c[0])
                        find = True
                        break
    return tables

def parse_sql(sql, conn, db_name, schema_path, table_columns, schema = 'public') :
    parsed_sql = sqlparse.parse(normalize_sql(sql))
    
    where_predicates = []
    multi_where_predicates = []
    other_predicates = []
    group_order_columns = []
    
    aggregation_func = ""
    aggregation_bool = False
    multi_where_predicate = ""
    
    is_and = False
    is_or = False
    is_from = False
    is_between = False 
    group_order = False
    
    column_names = []
    for columns in table_columns.values() :
        column_names.extend(columns)
    
    # print(column_names)
    used_tables = []
    
    ## get alias from sql ##
    simple_alias, complex_alias = extract_alias(sql, conn, table_columns)
    alias = {**simple_alias, **complex_alias}
    # print(simple_alias, alias)
    
    ## parse sql ##
    if len(parsed_sql) == 1 :        
        parsed_sql = parsed_sql[0]
        
        if 'intersect' in parsed_sql.value.lower().split() :
            if 'intersect' in parsed_sql.value : sqls = parsed_sql.value.split(' intersect ')
            else : sqls = parsed_sql.value.split(' INTERSECT ')
            
            for sql in sqls :
                info_, _, _ = parse_sql(sql, conn, db_name, schema_path, table_columns, schema)
                where_predicates.extend(info_['where_predicates'])
                other_predicates.extend(info_['other_predicates'])
                multi_where_predicates.extend(info_['multi_where_predicates'])
                group_order_columns.extend(info_['group_order_columns'])
                
        else :            
            ## extract predicates
            for token in parsed_sql.tokens :
                # print(token, token.__class__, token.ttype)
                predicate = ""
                
                subquery = extract_subqueries(token)
                if subquery != [] : # parse sql for subquery
                    for subq in subquery :
                        info_, sa_, ca_ = parse_sql(subq.value[1:-1].strip(), conn, db_name, schema_path, table_columns, schema)
                        where_predicates.extend(info_['where_predicates'])
                        other_predicates.extend(info_['other_predicates'])
                        multi_where_predicates.extend(info_['multi_where_predicates'])
                        group_order_columns.extend(info_['group_order_columns'])
                        simple_alias.update(sa_)
                        complex_alias.update(ca_)
                        alias = {**simple_alias, **complex_alias}
                    # print(token.value)
                    # continue
                
                if token.ttype is sqlparse.tokens.Keyword : # functions or group/order by
                    if token.value.lower() in sql_functions :
                        aggregation_func = token.value
                        aggregation_bool = True
                    elif token.value.lower() in ['group by', 'order by'] :
                        group_order = True
                    elif token.value.lower() == 'from' :
                        is_from = True
                    continue
                elif isinstance(token, sqlparse.sql.Comparison) : # join condition / filter subquery
                    predicate, where = get_predicate_str(token, aggregation_func, simple_alias, db_name, used_tables, table_columns, alias, schema)
                    if aggregation_func != "" : aggregation_func = ""
                    if predicate != "" : 
                        if where : where_predicates.append(predicate)
                        else : other_predicates.append(predicate)
                elif isinstance(token, sqlparse.sql.Where) : # where condition
                    aggregation_func = ""
                    where_predicate_str = ""
                    predicate = ""
                    for t in token :
                        # print(t, t.__class__, t.ttype, where_predicate_str)
                        if t.ttype is sqlparse.tokens.Keyword and t.value.lower() in sql_functions:
                            aggregation_func = t.value
                            continue
                        elif isinstance(t, sqlparse.sql.Comparison) :
                            predicate, where_condition = get_predicate_str(t, aggregation_func, simple_alias, db_name, used_tables, table_columns, alias, schema)
                            if aggregation_func != "" : aggregation_func = ""
                        elif isinstance(t, sqlparse.sql.Token) :
                            if t.value.lower() == 'where' :
                                continue
                            elif t.value.lower() in ["and", "or"] or t == token[-1] : 
                                # print(f"-- {t.value} --")
                                if t.value.lower() in ["and", "or"] and ('between' not in where_predicate_str.lower() or is_between):
                                    if t.value.lower() == "and" : is_and = True
                                    elif t.value.lower() == "or" : is_or = True
                                else : 
                                    if 'between' in where_predicate_str.lower() :
                                        is_between = True
                                    if not (t.value.lower() == "and" and 'and' in where_predicate_str.lower()) : where_predicate_str += t.value                                
                                    if t != token[-1] : continue
                                    if t == token[-1] and t.value == ';' : where_predicate_str = where_predicate_str
                                     
                                if where_predicate_str.strip()!= "" : 
                                    if where_predicate_str.lower().strip().startswith('and' or 'or') :
                                        where_predicate_str = where_predicate_str[3:]
                                    where_predicate_str = get_preidcate(where_predicate_str, simple_alias, used_tables, table_columns)
                                    
                                    if where_predicate_str != "" :
                                        # print(where_predicate_str)
                                        where_predicates.append(where_predicate_str)
                                        if multi_where_predicate == "" :  
                                            multi_where_predicate = where_predicate_str # start of current multi predicates
                                        elif is_and and where_predicate_str not in multi_where_predicate : multi_where_predicate += ' and ' + where_predicate_str
                                        elif is_or and where_predicate_str not in multi_where_predicate : multi_where_predicate += ' or ' + where_predicate_str

                                        is_or = False
                                        is_and = False
                                        if 'between' in where_predicate_str.lower() : is_between = False
                                        where_predicate_str = ""
                                    continue
                            elif isinstance(t, sqlparse.sql.Parenthesis) :
                                if t.value.strip().startswith('(') and t.value.strip().endswith(')') and 'select' not in t.value.lower() :
                                    if aggregation_func == "" : predicates_ = t.value[1:-1].strip()
                                    else : predicates_ = aggregation_func + t.value
                                    if is_literal_list('[' + predicates_ + ']') or any(i not in predicates_.lower() for i in ['and', 'or', 'select']) :
                                        if is_literal_list('[' + predicates_ + ']') : where_predicate_str += t.value
                                        else : where_predicate_str += predicates_
                                    else :
                                        fake_sql = f"SELECT * FROM table WHERE {predicates_}"
                                        sub_info, _, _ = parse_sql(fake_sql, conn, db_name, schema_path, table_columns, schema)
                                        where_predicates = list(set(where_predicates).union(set(sub_info['where_predicates'])))
                                        other_predicates = list(set(other_predicates).union(set(sub_info['other_predicates'])))
                                        multi_where_predicates = list(set(multi_where_predicates).union(set(sub_info['multi_where_predicates'])))
                                        group_order_columns = list(set(group_order_columns).union(set(sub_info['group_order_columns'])))
                                    aggregation_func = ""
                                elif t.value.strip().startswith('(') and t.value.strip().endswith(')') and 'select' in t.value.lower() :
                                    where_predicate_str = ""
                                    continue
                                else :
                                    where_predicate_str += t.value
                            else :
                                if where_predicate_str.strip() == "" and t.value.lower() in ['and', 'or'] : continue
                                elif isinstance(t, sqlparse.sql.Operation) :
                                    if aggregation_func != "" : 
                                        where_predicate_str += aggregation_func
                                        aggregation_func = ""
                                    where_predicate_str += t.value
                                    continue
                                elif isinstance(t, sqlparse.sql.Identifier) and ')' in t.value :
                                    where_predicate_str += t.value.split(')')[0]
                                    continue
                                where_predicate_str += t.value
                        
                            
                        if predicate != "" :
                            if where_condition : 
                                where_predicates.append(predicate)
                                if multi_where_predicate == "" :
                                    multi_where_predicate = predicate # start of current multi predicates
                                    continue
                                elif is_and and predicate not in multi_where_predicate : multi_where_predicate += ' and ' + predicate
                                elif is_or and predicate not in multi_where_predicate : multi_where_predicate += ' or ' + predicate
                                    
                                is_or = False
                                is_and = False
                            else : other_predicates.append(predicate)
                            predicate = ""
                elif isinstance(token, sqlparse.sql.Identifier) :
                    if is_from :
                        for table in table_columns.keys() :
                            if table in token.value : used_tables.append(table)
                    elif group_order : # add colunmn name to group_order_cols
                        # print(token.value)
                        tv = token.value
                        if 'desc' in token.value.lower() :
                            tv = tv.replace('desc', ' ').replace('DESC', ' ').strip()
                        elif 'asc' in token.value.lower() :
                            tv = tv.replace('asc', ' ').replace('ASC', ' ').strip()
                        
                        original_value = ""
                        if tv in column_names :
                            original_value = tv
                        elif len(token.value.strip().split()) == 1 and '.' in token.value :
                            original_value = token.value.split('.')[-1]
                        else : # single column name with alias    
                            if tv in alias.keys() :
                                temp = alias[tv]
                                if isinstance(temp, tuple) :
                                    if len(temp) == 2 : original_value = temp[1]
                                elif isinstance(temp, str) : # find all column_name in this expression
                                    for cn in column_names :
                                        if cn in temp : original_value = cn
                                else :
                                    logger.debug(f"** Error type of {temp}, {type(temp)}")
                            # else :
                            #     logger.error(f"** Error alias of {tv}")  
                        
                        if original_value != "" :
                            for table_name, cols in table_columns.items() : 
                                if original_value in cols : 
                                    group_order_columns.append(f"{table_name}.{original_value}")

                        group_order = False
                    elif token.value.lower().startswith('case when ') :
                        tmp_predicate = token.value.lower().split('case when ')[-1].split('then')[0].strip()
                        fake_sql = f"SELECT * FROM table WHERE {tmp_predicate} ;"
                        # print(fake_sql)
                        sub_info, _, _ = parse_sql(fake_sql, conn, db_name, schema_path, table_columns, schema)
                        where_predicates = list(set(where_predicates).union(set(sub_info['where_predicates'])))
                        other_predicates = list(set(other_predicates).union(set(sub_info['other_predicates'])))
                        multi_where_predicates = list(set(multi_where_predicates).union(set(sub_info['multi_where_predicates'])))
                        group_order_columns = list(set(group_order_columns).union(set(sub_info['group_order_columns'])))
                                    
                elif isinstance(token, sqlparse.sql.IdentifierList) :
                    if is_from : 
                        for table in table_columns.keys() :
                            if table in token.value : used_tables.append(table)
                    if group_order : # add colunmn name to group_order_cols
                        for t in token :
                            if re.fullmatch(identifier_pattern, t.value) or re.fullmatch(table_dot_column, t.value) :
                                original_value = ""
                                if t.value.lower() in sql_functions :
                                    aggregation_func = t.value
                                    continue
                                elif t.value in column_names :
                                    original_value = t.value
                                elif len(t.value.strip().split()) == 1 and '.' in t.value :
                                    original_value = t.value.split('.')[-1]
                                else : # single column name with alias
                                    tv = aggregation_func + " " + t.value
                                    tv = tv.strip()
                                      
                                    if tv in alias.keys() :
                                        temp = alias[tv]
                                        if isinstance(temp, tuple) :
                                            if len(temp) == 2 : original_value = temp[1]
                                        elif isinstance(temp, str) : # find all column_name in this expression
                                            for cn in column_names :
                                                if cn in temp : original_value = cn
                                        else :
                                            logger.debug(f"** Error type of {temp}, {type(temp)}")
                                    else :
                                        logger.debug(f"** Error alias of {tv}")  
                                    aggregation_func = ""
                        
                                if original_value != "" :
                                    for table_name, cols in table_columns.items() : 
                                        if original_value in cols : 
                                            group_order_columns.append(f"{table_name}.{original_value}")

                        group_order = False
                if aggregation_bool == True and aggregation_func != "" : # for the case that mis-func
                    aggregation_bool = False
                    aggregation_func = ""
                
            if multi_where_predicate != '' : 
                if multi_where_predicate.startswith(" and ") :
                    multi_where_predicate = multi_where_predicate[4:].strip()
                elif multi_where_predicate.startswith(" or ") :
                    multi_where_predicate = multi_where_predicate[3:].strip()

                multi_where_predicates.append(multi_where_predicate)
    else :
        print("Error: SQL statement has more than one statement")
        print(normalize_sql(sql))
        exit()
        
    # print(where_predicates)
    # print(other_predicates)
    # print(group_order_columns)
    # print(simple_alias)
    
    multi_where_predicates =[mwp for mwp in multi_where_predicates if mwp not in where_predicates]
       
    ## where_predicates 
    where_predicates_ = where_predicates.copy()   
    where_predicates = []
    for wp in where_predicates_ :
        exp_list = wp.split()
        exp = ""
        for exp_ in exp_list :
            if re.fullmatch(identifier_pattern, exp_) and exp_.lower() not in sql_keywords :
                find = False
                for tab in used_tables :
                    if exp_ in table_columns[tab] :
                        exp += f"{tab}.{exp_} "
                        find = True
                        break
                if not find : exp += f"{exp_} "
            else : exp += exp_ + " "
        where_predicates.append(exp)                                
      
    ## aggregate all parsed information
    info = {}
    info["where_predicates"] = where_predicates
    info["other_predicates"] = other_predicates
    info["multi_where_predicates"] = multi_where_predicates
    info["group_order_columns"] = group_order_columns
    # print(info, simple_alias, complex_alias)
    
    return info, simple_alias, complex_alias
  
def obtain_sql_information(sqls, conn, db_name, schema_path, schema = 'public') : # -> where_selectivities, join_predicates, group_order_cols
    cardinality_sqls = []
    tables_sqls = []
    info = {}
        
    global parsing_time
    stime = time.time()
    ## get_schema ##
    if not os.path.exists(schema_path) :
        get_db_schema(db_name, schema_path, schema)
    with open(schema_path, "r") as f :
        schema_info = json.load(f)["table_info"]
    
    ## get table names
    table_cols = get_tables_columns_names(schema_path)
    table_names = list(table_cols.keys())
    table_rows = {table_info['table'] : table_info['rows'] for table_info in schema_info}
    # print(table_names)
    column_names = []
    for columns in table_cols.values() :
        column_names.extend(columns)   
        
    ## predicates [where(contains multi_where), other, group_order] ##
    parsed_infos, simple_alias, _ = parse_sql(sqls, conn, db_name, schema_path, table_cols, schema)
    where_predicates = list(set(parsed_infos['where_predicates'])) 
    # list(set(parsed_infos['where_predicates']).union(set(parsed_infos['multi_where_predicates'])))
    other_predicates = parsed_infos['other_predicates']
    group_order_columns = parsed_infos['group_order_columns']
    # print(f"** where_predicates is {where_predicates}, {len(where_predicates)}")
    # print(f"** other_predicates is {other_predicates}, {len(other_predicates)}")
    # print(f"** group_order_columns is {group_order_columns}, {len(group_order_columns)}")
    etime = time.time()
    parsing_time += etime - stime
    
    ## selectivities_for_where_predicates
    where_predicates_ = []
    cardinalities = []
    total_lines = []
    selectivities = []
    for predicate in where_predicates :
        ## get table names
        tables = find_table_info(db_name, schema_path, predicate)
        card_sql = "explain (format json) select * from "
        for table in tables :
            card_sql += table + ", "
        if tables == [] :
            continue
        card_sql = card_sql[:-2] + " where " + predicate + ";"
        # print(sql)
        
        result = execute_sql(card_sql, db_name, schema)
        if result != [] :
            card = result[0][0][0]['Plan']['Plan Rows']
            total_num = 0
            for table in tables :
                if table in table_names :
                    total_num += table_rows[table]
                else :
                    # print(f"Error : table {table} not found in schema")
                    continue
            if total_num != 0 and card / total_num < 1:
                tables_sqls.append(tables)
                cardinality_sqls.append(card_sql)  
                cardinalities.append(card)
                total_lines.append(total_num)
                where_predicates_.append(predicate)
                selectivities.append(card / total_num)
                # selectivities.append(round(card / total_num, 4))
        else : 
            if "max" in predicate.lower() or "min" in predicate.lower() :
                # extract aggregation expression
                agg = ""
                if "max" in predicate : agg = 'max'
                elif "MAX" in predicate : agg = 'MAX'
                elif 'min' in predicate : agg = 'min'
                else : agg = "MIN"

                agg_expression = agg
                tmp_predicate = predicate.split(agg)[-1]
                if tmp_predicate.strip().startswith('(') :
                    cnt = 0
                    for token in tmp_predicate :
                        if token == '(' : cnt += 1
                        elif token == ')' : cnt -= 1

                        agg_expression += token

                        if cnt == 0 : break
                
                column_info = agg_expression.split('(')[-1].split(')')[0]

                card_sql = "explain (format json) select * from "
                for table in tables :
                    card_sql += table + ", "
                if tables == [] :
                    continue
                card_sql = card_sql[:-2] + f" where {column_info} = (select {agg_expression} from " 
                for table in tables :
                    card_sql += table + ", "
                card_sql = card_sql[:-2] + ");"

                result = execute_sql(card_sql, db_name, schema)
                if result != [] :
                    card = result[0][0][0]['Plan']['Plan Rows']
                    total_num = 0
                    for table in tables :
                        if table in table_names :
                            total_num += table_rows[table]
                        else :
                            # print(f"Error : table {table} not found in schema")
                            continue
                    if total_num != 0 and card / total_num < 1:
                        tables_sqls.append(tables)
                        cardinality_sqls.append(card_sql)  
                        cardinalities.append(card)
                        total_lines.append(total_num)
                        where_predicates_.append(predicate)
                        selectivities.append(card / total_num)
                        # selectivities.append(round(card / total_num, 4))
        

        # else : 
        #     logger.error(f"* error cardinality sql : {card_sql}")
        
        
    ## group_order, join_predicates
    group_order_columns_ = {}
    for goc in group_order_columns :
        goc = goc.strip()
        if goc in group_order_columns_.keys() :
            group_order_columns_[goc] += 1
        else :
            group_order_columns_[goc] = 1
    
    other_predicates_ = {}
    for op in other_predicates :
        ops = op.split()
        for ops_ in ops :
            ops_ = ops_.strip()
            if re.fullmatch(table_dot_column, ops_) :
                tab = ops_.split('.')[0]
                col = ops_.split('.')[1]
                if tab in simple_alias.keys() :
                    tab = simple_alias[tab][0]
                if col in simple_alias.keys() :
                    tab = simple_alias[col][0]
                    col = simple_alias[col][1]
                if col in column_names and tab not in table_names :
                    for table, cols in table_cols.items() :
                        if col in cols : 
                            tab = table
                            break
                elif col not in column_names or tab not in table_names :
                    continue
                ops_ = tab + '.' + col
                if ops_ in other_predicates_.keys() :
                    other_predicates_[ops_] += 1
                else :
                    other_predicates_[ops_] = 1

    info['where_selectivities'] = dict(zip(where_predicates_, selectivities))
    info['other_predicates'] = other_predicates_
    info['group_order_columns'] = group_order_columns_
    # print(f"where_selectivities : {len(info['where_selectivities'].keys())}")
    # print(f"other_predicates : {len(info['other_predicates'])}")
    # print(f"group_order_columns : {len(info['group_order_columns'])}")
    # print(info)
    
    return info

def aggregate_sqls_information(workload_info, sql_info) : # where_selectivities, other_predicates, group_order_columns
    workload_info['where_selectivities'] = {**workload_info['where_selectivities'], **sql_info['where_selectivities']}
    
    other_predicates = workload_info['other_predicates']
    other_predicates_ = sql_info['other_predicates']
    for op_ in other_predicates_.keys() :
        if op_ in other_predicates.keys() :
            other_predicates[op_] += other_predicates_[op_]
        else :
            other_predicates[op_] = other_predicates_[op_]
    workload_info['other_predicates'] = other_predicates
    
    group_order_columns = workload_info['group_order_columns']
    group_order_columns_ = sql_info['group_order_columns']
    for goc_ in group_order_columns_.keys() :
        if goc_ in group_order_columns.keys() :
            group_order_columns[goc_] += group_order_columns_[goc_]
        else :
            group_order_columns[goc_] = group_order_columns_[goc_]
    workload_info['group_order_columns'] = group_order_columns
    
    return workload_info

def aggregate_per_sql_information(workload_info, sql_info) : # where_selectivities, other_predicates, group_order_columns
    workload_info['where_selectivities'].append(sql_info['where_selectivities'])
    
    other_predicates = workload_info['other_predicates']
    other_predicates_ = sql_info['other_predicates']
    for op_ in other_predicates_.keys() :
        if op_ in other_predicates.keys() :
            other_predicates[op_] += other_predicates_[op_]
        else :
            other_predicates[op_] = other_predicates_[op_]
    workload_info['other_predicates'] = other_predicates
    
    group_order_columns = workload_info['group_order_columns']
    group_order_columns_ = sql_info['group_order_columns']
    for goc_ in group_order_columns_.keys() :
        if goc_ in group_order_columns.keys() :
            group_order_columns[goc_] += group_order_columns_[goc_]
        else :
            group_order_columns[goc_] = group_order_columns_[goc_]
    workload_info['group_order_columns'] = group_order_columns
    
    return workload_info

def obtain_sql_info(sql, conn, db_name, schema_path, idx, used_column_info_rows, workload_infos, lock, schema = 'public') : 
    used_column_info_, used_column_info_rows_, _, _, _, _ = schema_pruning(db_name, sql, schema_path)
    used_column_info_rows = used_column_info_rows.extend(used_column_info_rows_)
    # print(used_column_info_)
    
    view = False
    drop_view = ""
    if "create view" in sql : # for TPC-H create view query
        view = True
        sqls = sql.split(';')
        if len(sqls) == 4:
            execute_sql_view(sqls[0] + ';', conn)
            sql = sqls[1] + ';' 
            drop_view = sqls[2] + ';'
        else :
            logger.error("Error in create view query")
            exit()       
    
    parsed_infos = obtain_sql_information(sql, conn, db_name, schema_path, schema)
    with lock :
        workload_infos = aggregate_sqls_information(workload_infos, parsed_infos)

        # print([column.split(':')[0].strip() for column in used_column_info_])
        workload_infos['sql_columns'][f'SQL_{idx}'] = [column.split(':')[0].strip() for column in used_column_info_]
    
    if view :
        execute_sql_view(drop_view, conn)
        view = False
 
    semaphore.release()
    # print(used_column_info)
    return workload_infos, used_column_info_rows

def obtain_workload_information(workload, conn, db_name, schema_path, schema = 'public', multi_pro = 20) : 
    # used_columns, where_selectivities, other_predicates, group_order_columns
    global semaphore 
    semaphore = Semaphore(multi_pro)
    
    processes = []
    manager = Manager()
    used_column_info_rows = manager.list()
    workload_infos = manager.dict({"where_selectivities" : {}, "other_predicates" : {}, "group_order_columns" : {}, "sql_columns" : {}})
    workload_infos['sql_columns'] = manager.dict()

    lock = manager.Lock()
    
    for sql_index, sql in enumerate(workload):
        semaphore.acquire() 
        
        process = Process(
            target=obtain_sql_info,
            args=(sql, conn, db_name, schema_path, sql_index, used_column_info_rows, workload_infos, lock, schema)
        )
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
      
    workload_infos["sql_columns"] = dict(sorted(dict(workload_infos["sql_columns"]).items(), key=lambda item: extract_number(item[0])))
    used_column_info_rows = list(set(used_column_info_rows))
    
    return workload_infos, used_column_info_rows
    
if __name__ == "__main__" :    
    workload_path = "/home/lihaoyang.cs/zhaoxinxin/default_version/test.sql"
    with open(workload_path, 'r') as file :
        workload = file.readlines()
    
    test_sql = "select id, create_time, regression_task_id, case_id, running_task_id, running_status, is_passed, expected_indexes, real_indexes from sqlbrain_regression_details where case_id = 'EEeb' order by id desc limit 10 offset 10;"
    db_name = 'sqlbrain_tp'
    schema_path = "/home/lihaoyang.cs/zhaoxinxin/default_version/gpt4index/data/schema/sqlbrain_tp_schema.json"
    schema = "bytebrain_data_source"
    
    config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = config.user,
        password = config.password,
        host = config.host,
        port = config.port
    ) 

    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    conn.commit()
    cursor.close()

    db_name = 'sqlbrain_ap'
    schema_path = "/home/lihaoyang.cs/zhaoxinxin/gpt4index/data/schema/sqlbrain_ap_schema.json"

    get_db_schema(db_name, schema_path, schema)
    exit()
    
    info = obtain_sql_information(test_sql, conn, db_name, schema_path)
    print(info)
    exit()
    
    parsing_time = 0
    start_time = time.time()
    workload_infos, used_column_info = obtain_workload_information(workload, conn, db_name, schema_path)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(workload_infos)
    print(used_column_info)
    print(f"Execution time: {execution_time:.6f} seconds")
    print(f"Parsing time: {parsing_time:.6f} seconds")