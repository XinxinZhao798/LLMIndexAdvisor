import json 
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests
import sqlparse
import os
import re
from collections.abc import Iterable
import subprocess
import time
import random
import numpy as np
from sklearn.cluster import KMeans
from multiprocessing import Process, Manager, Semaphore, Lock
from datetime import datetime, timezone, timedelta

from config import DBConfig
from utils.db_utils import get_table_names_from_db, execute_sql, count_tp_nums_json

import logging

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
identifier_pattern = r'\b[a-zA-Z_][a-zA-Z0-9_]*\b'
table_dot_column = r'\b[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b'
constant_pattern = r"('(?:''|[^'])*')|(\b\d+\b)|(\b\d+\.\d+\b)|(\bTRUE\b|\bFALSE\b|\bNULL\b)"
sql_functions = [
    "sum", "count", "avg", "max", "min", "extract", "group_concat", "string_agg", "variance", "stddev",
    "median", "percentile_cont", "percentile_disc", "abs", "ceiling", "ceil", "floor", "round", "power", 
    "exp", "extract", "log", "sqrt", "sin", "cos", "tan", "concat", "substring", "substr", "upper", "lower",
    "trim", "length", "replace", "lpad", "rpad", "now", "current_date", "current_time", "date_add", "date_sub",
    "date_part", "to_date", "to_char", "coalesce", "nullif", "case", "greatest", "least",
    "json_extract", "json_agg", "xmlagg", "st_distance", "st_intersects", "st_union",
    "row_number", "rank", "dense_rank", "lead", "lag", "ntile"
]

create_index_regex = re.compile(
    r"(?i)CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(USING\s+\w+\s*)?\(\s*([\w\.\(\)\s,]+)\s*\)\s*;?",
    re.IGNORECASE
)
drop_index_regex = re.compile(
    r"DROP\s+INDEX\s+(IF\s+EXISTS\s+)?(\w+)\s*(CASCADE|RESTRICT)?",
    re.IGNORECASE
)

logger = logging.getLogger('log')
root_dir = os.path.dirname(os.path.abspath(__file__))

def execute_sql_view(sql, conn):
    results = None
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout TO 360000;")
        cur.execute(sql)
        conn.commit()
        cur.close()
    except psycopg2.Error as e:
        # print(sql)
        logger.error(f"An error occurred: {e}")
    
    return results

def execute_sql_com(sql, conn):
    results = None
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout TO 360000;")
        cur.execute(sql)
        results = cur.fetchall()
        # print(results)
    except psycopg2.Error as e:
        print(sql)
        print(f"An error occurred: {e}")
    finally:
        conn.commit()
        cur.close()
    
    return results

def get_db_info(db_name, table_names, schema = 'public'):
    # get information each table (including CREATE TABLE, FOREIGN KEY, PRIMARY KEY, etc.)
    create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements = [], [], [], []
    for table_name in table_names:
        os.environ['PGPASSWORD'] = DBConfig.password # [Path]
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
                    create_table_statements.append(stmt.strip().replace("public.", "") + ";")
                elif "PRIMARY KEY" in stmt.upper():
                    primary_key_constraints.append(stmt.strip().replace("public.", "") + ";")
                elif "FOREIGN KEY" in stmt.upper():
                    foreign_key_constraints.append(stmt.strip().replace("public.", "") + ";")
        else:
            print(f"Error getting DDL for table {table_name}: {error}")

        get_indexes_sql = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{}';".format(table_name)
        for res in execute_sql(get_indexes_sql, db_name):
            create_index_statements.append(res[1].replace("public.", ""))

    create_table_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_table_statements]
    primary_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in primary_key_constraints]
    foreign_key_constraints = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in foreign_key_constraints]
    create_index_statements = [sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower') for sql in create_index_statements]

    return create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements
 
def is_idx_sql(query):
    if len(query.split()) > 20 or len(query.split()) <= 3 : return False
    try:
        parsed = sqlparse.parse(query)
        return len(parsed) > 0
    except Exception:
        return False
   
def get_llm_response(api_key, model_name, system_mes, usr_mes, temperature, num_of_samples):
    url = 'http://10.77.110.151:10000/get_response'
    params = {
        'api_key': api_key,
        'model_name': model_name,
        'system_mes': system_mes,
        'usr_mes': usr_mes,
        'temperature': temperature,
        'num_of_samples': num_of_samples
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None
     
def parse_message(message) :
    mes = str(message).split('\\n')
    idxes = []
    form = True
    
    ## get idxes
    if len(mes) == 1 :
        mes = str(message).split('\n')
    mes = [m.strip() for m in mes]
    for m in mes :
        if 'CREATE INDEX ' in m:
            sql = 'CREATE INDEX' + m.split('CREATE INDEX')[1].split(')')[0] + ') ;'
            if create_index_regex.match(sql) :
                if sql != '' and is_idx_sql(sql) : idxes.append(sql)
        elif 'DROP INDEX ' in m:
            sql = 'DROP INDEX' + m.split('DROP INDEX')[1].split(';')[0] + ';'
            if drop_index_regex.match(sql) :
                if sql != '' and is_idx_sql(sql) and len(sql.split()) < 10 : idxes.append(sql)

    if len(idxes) == 0 : 
        logger.warning("* Maybe Recommendation Finished !")
        form = False
        idxes.append(message)
    return idxes, form

def execute_sql_index_creation(conn, indexes) :
    indexes_ = indexes.copy()
    with conn.cursor() as cur :
        if indexes == [] :
            logger.error("Error with empty indexes set!")
            return

        ## index creation
        for idx_creation in indexes :
            if not idx_creation.startswith('--') and not idx_creation.strip() == '' :
                try :
                    cur.execute(idx_creation)
                except Exception as e:
                    logger.error(f"Indexes creation failed : {idx_creation}")
                    indexes_.remove(idx_creation)
                conn.commit()
        logger.info(f"Index creation finished!")
        
    return indexes_
        
def execute_sql_file(conn, sql_file_path, schema = "public", threshold = 5000, one=True):
    execution_time = 0
    with conn.cursor() as cur:
        cur.execute(f"set search_path to {schema}")
        with open(sql_file_path, 'r') as sql_file:
            sql_commands = sql_file.readlines()
        sql_commands = [line.strip() for line in sql_commands if line.strip() and not line.strip().startswith('--')]
        
        for sql in sql_commands :
            if "create view" in sql :
                cur.execute(sql) # execute only
                conn.commit()
            else :
                # print(sql)
                try :
                    cur.execute("SET statement_timeout = %s", (threshold * 1000,))
                    if sql.lower().split()[0] in ['select', 'insert', 'update', 'delete', 'with'] : 
                        cur.execute(f"explain (analyze, buffers, format json) {sql}")
                        if one : 
                            plan = cur.fetchone()[0][0]["Plan"]
                        else : plan = cur.fetchall()[0][0]["Plan"]
                        tmp = plan["Actual Total Time"]
                        execution_time += plan["Actual Total Time"]
                        # logger.debug(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
                        print(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
                    else :
                        cur.execute(sql)
                except psycopg2.OperationalError as error :
                    logger.debug(f"* current sql has timeout --> {sql}")
                    execution_time += threshold * 1000
                    conn.rollback()
                    continue

    return execution_time
 
def execute_sql_file_bar(conn, sql_file_path, threshold, one=True):
    execution_time = 0

    with conn.cursor() as cur:
        ## workload execution
        with open(sql_file_path, 'r') as sql_file:
            sql_commands = sql_file.readlines()
        sql_commands = [line.strip() for line in sql_commands if line.strip() and not line.strip().startswith('--')]
        
        for idx, sql in enumerate(sql_commands) :
            if "create view" in sql :
                cur.execute(sql) # execute only
                conn.commit()
            else :
                # print(sql)
                try :
                    cur.execute("SET statement_timeout = %s", (threshold * 1000,))
                    cur.execute(f"explain (analyze, buffers, format json) {sql}")
                    if one : 
                        plan = cur.fetchone()[0][0]["Plan"]
                    else : plan = cur.fetchall()[0][0]["Plan"]

                    tmp = plan["Actual Total Time"]
                    execution_time += plan["Actual Total Time"]
                    if execution_time >= threshold * 1000 : 
                        logger.info(f"* current sql idx is {idx}.")
                        break
                    logger.debug(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
                except psycopg2.OperationalError as error :
                    logger.debug(f"* current sql has timeout --> {sql}")
                    execution_time += threshold * 1000
                    conn.rollback()
                    break

    return execution_time

def drop_index_prefix(conn, SQL = "select indexname from pg_indexes where schemaname NOT IN ('pg_catalog', 'information_schema') and indexname not like '%_pkey';" ) :
    index_name = SQL
    index_name = "SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public' AND indexname NOT IN (SELECT conname FROM pg_constraint WHERE contype = 'p');"
    # print(index_name)  
    cur = conn.cursor()
    cur.execute(index_name)
    indexes = cur.fetchall()
    for indexname, tablename in indexes :
        try:
            drop_stmt = "drop index {}".format(indexname)
            cur.execute(drop_stmt)
        except psycopg2.Error as e:
            logger.error(f"Error dropping index {indexname}: {e}")
            conn.rollback()
            # 如果删除索引失败，尝试删除约束
            constraint_stmt = f"SELECT conname FROM pg_constraint WHERE conrelid = '{tablename}'::regclass AND conindid = '{indexname}'::regclass;"
            cur.execute(constraint_stmt)
            constraint = cur.fetchone()
            
            if constraint:
                constraint_name = constraint[0]
                try :
                    drop_constr_stmt = f"ALTER TABLE {tablename} DROP CONSTRAINT {constraint_name}"
                    # print(drop_constr_stmt)
                    cur.execute(drop_constr_stmt)
                    drop_stmt = "drop index if exists {}".format(indexname)
                    cur.execute(drop_stmt)
                except psycopg2.Error as e :
                    conn.rollback()
                    continue
            else:
                print(f"No constraint found for index {indexname} on table {tablename}.")
            
            continue
        
    conn.commit()

def oltp_stress_test_db(benchmark, benchmark_config) :
    stamp = int(time.time())
    
    config = DBConfig()
    connection = psycopg2.connect(
        dbname = 'u2023000897',
        user = config.user,
        password = config.password,
        host = config.host,
        port = config.port
    ) 
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with connection.cursor() as cur :
        cur.execute("drop database if exists benchbase ;")
        cur.execute("create database benchbase;")
    connection.close()
    
    connection = psycopg2.connect(
        dbname = 'benchbase',
        user = config.user,
        password = config.password,
        host = config.host,
        port = config.port
    ) 
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with connection.cursor() as cur :
        cur.execute("create extension hypopg;")
    connection.close()
        
    cd_cmd = "cd /home/u2023000897/benchbase/target/benchbase-postgres"
    result_file = '/home/u2023000897/benchbase/target/benchbase-postgres/results/{}/{}'.format(benchmark, stamp)
    # exe_cmd = 'java -jar benchbase.jar -b {} -c config/postgres/{}/sample_{}_config{}.xml --clear=true --create=true --load=true --execute=true --directory {}'.format(benchmark, benchmark, benchmark, workload, result_file)
    exe_cmd = 'java -jar benchbase.jar -b {} -c {} --clear=true --create=true --load=true --directory {}'.format(benchmark, benchmark_config, result_file)

    
    os.system(f"{cd_cmd} && {exe_cmd}")
    
    return 1

def oltp_stress_test(benchmark, config) :
    stamp = int(time.time())

    cd_cmd = "cd /home/u2023000897/benchbase/target/benchbase-postgres"
    result_file = '/home/u2023000897/benchbase/target/benchbase-postgres/results/{}/{}'.format(benchmark, stamp)
    # exe_cmd = 'java -jar benchbase.jar -b {} -c config/postgres/{}/sample_{}_config{}.xml --clear=true --create=true --load=true --execute=true --directory {}'.format(benchmark, benchmark, benchmark, workload, result_file)
    exe_cmd = 'java -jar benchbase.jar -b {} -c {} --execute=true --directory {}'.format(benchmark, config, result_file)

    # print(f"{cd_cmd} && {exe_cmd}")
    # exit()
    
    os.system(f"{cd_cmd} && {exe_cmd}")

    for file in os.listdir(result_file):
        if file.endswith('summary.json'):
            results = json.load(open(f'{result_file}/{file}'))
            result = results['Throughput (requests/second)']

    return result

## query plan pruning
def format_plan(node):
    # type
    formatted = f"{node['Node Type'].lower()}"
    # check child
    if 'Plans' in node:
        child_formats = []
        for child in node['Plans']:
            child_format = format_plan(child)
            child_formats.append(child_format)
        cost = node['Total Cost'] # - node['Startup Cost']
        if cost < 1000:
            cost = format(cost, '.1f')
        elif cost > 1000 and cost < 1000000:
            cost = f'{int(cost/1000)}k'
        elif cost >= 1000000:
            cost = f'{int(cost/1000000)} million'
        formatted += f"({', '.join(child_formats)}, {cost})"
        # print(formatted)

    return formatted

def card_estimation(plan): 
    # Parameter : Query_plan[0]['Plan']
    cardinality = []
    if plan['Node Type'] in ['Seq Scan', 'Index Scan', 'Bitmap Heap Scan', 'Bitmap Index Scan', 'Index Only Scan'] and 'Alias' in plan:
        card = (plan['Alias'], plan['Plan Rows'])
        cardinality.append(card)
    if 'Plans' in plan:
        for subplan in plan['Plans']:
            cardinality.extend(card_estimation(subplan))
    
    return cardinality

## get table info [table_names --> rows, column_names]
def get_table_info(conn, db_name, schema = 'public') :
    rows_path = f"./data/info/{db_name}_info.json"
    if os.path.exists(rows_path) :
        with open(rows_path, 'r') as f :
            info = json.load(f)
    else :
        info = {}
        cursor = conn.cursor()
        cursor.execute(f"set search_path to {schema};")
        cursor.execute("analyze;")
        
        sql = f"""SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public';
            """
        cursor.execute(sql)
        tables = cursor.fetchall()
        
        tp_nums = count_tp_nums_json(db_name,"./data/tp_nums.json")
        
        for table in tables :
            table = table[0]
            info[table] = {}
            info[table]["tp_nums"] = tp_nums[table]
            sql = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';"
            cursor.execute(sql)
            columns = [row[0] for row in cursor.fetchall()]
            info[table]['column_names'] = columns
        
        with open(rows_path, 'w') as f:
            json.dump(info, f, indent=4)
    
    return info

def get_columns_names(schema_path) :
    columns = []
    with open(schema_path, 'r') as f :
        schema = json.load(f)
    for table in schema['table_info'] :
        for column in table['columns'] :
            columns.append(column['name'])
    return columns

def get_tables_names(schema_path) :
    tables = []
    with open(schema_path, 'r') as f :
        schema = json.load(f)
    for table in schema['table_info'] :
        tables.append(table["table"])
    return tables
              
## what-if
def query_plan_cost_estimation(sqls, conn, schema = 'public') :
    total_cost = 0
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    # cursor.execute("show search_path;")
    # result = cursor.fetchall()
    # print(result)
    cursor.execute("SELECT count(*) FROM hypopg_list_indexes;")
    logger.info(f"* all hypopg indexes --> {cursor.fetchall()}")
    view = False

    used_indexes = set([])

    for sql in sqls :
        if "create view" in sql : # for TPC-H create view query
            view = True
            sqls = sql.split(';')
            if len(sqls) == 4:
                cursor.execute(sqls[0] + ';')
                sql = sqls[1] + ';' 
                drop_view = sqls[2] + ';'
        try :
            if sql.lower().split()[0] in ['select', 'insert', 'update', 'delete', 'with'] :
                cursor.execute(f"explain (format json) {sql}")
                # print(f"explain (format json) {sql}")
                plan = cursor.fetchone()[0][0]['Plan']
                total_cost += plan['Total Cost']

                used_indexes_ = query_plan_get_index_name([plan])
                used_indexes = used_indexes.union(used_indexes_)
            else :
                cursor.execute(sql)
        except psycopg2.Error as e :
            logger.error(f"Error in EXPLAIN --> {sql}")
            conn.rollback()

        if view :
            cursor.execute(drop_view)
            view = False
    conn.commit()
    cursor.close()

    # logger.info(f"-- used_indexes --> {used_indexes}, {len(used_indexes)}")
    
    return total_cost

def hypopg_indexes_creation_constraint(sqls, conn, schema = 'public', storage_constraint=1000000) : # MB
    total_storage = 0
    constrain_sqls = []
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    
    for sql in sqls :
        if not sql.startswith('--') and sql.strip() != '' and not sql.lower().startswith("drop"):
            hypopg_creation = f"SELECT hypopg_create_index('{sql}');"
            try :
                cursor.execute(hypopg_creation)
                # print(hypopg_creation)
            except psycopg2.Error as e :
                logger.error(f"{e}")
                conn.rollback()
                continue
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            total_storage += cursor.fetchone()[0]/(1024*1024) ## B --> MB
            if total_storage > storage_constraint :
                cursor.execute(f"select hypopg_drop_index({indexrelid});")
                break
            constrain_sqls.append(sql)

    conn.commit()
    cursor.close()
    
    return constrain_sqls

def hypopg_drop_indexes(conn, schema = 'public') :
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    cursor.execute("select * from hypopg_list_indexes;")
    hypopg_indexes = cursor.fetchall()
    for index in hypopg_indexes :
        cursor.execute(f"select hypopg_drop_index({index[0]});")
    conn.commit()
    cursor.close()
  
## get default indexes
def obtain_default_index_statements(db_name, schema_path, schema = 'public') :
    # get tables
    tables = get_tables_names(schema_path)
    _, _, _, index_stats = get_db_info(db_name, tables, schema)
    
    # index statesment pruning
    indexes = []
    for index_stat in index_stats :
        match = re.fullmatch(create_index_regex, index_stat)
        if match :
            index_name = match.group(2)
            table_name = match.group(3)
            columns_name = [col.strip() for col in match.group(5).split(",")]
            index = index_name + '('
            for column in columns_name :
                index += table_name + '.' + column + ','
            index = index[:-1] + ')'
            indexes.append(index)
        else :
            print(f"Error index statements {index_stat}")
            exit()
    
    return index_stats, indexes
  
## incremental recommendation
def hypopg_create_existing_indexes(conn, existing_indexes_, schema = 'public') :
    existing_indexes = existing_indexes_.copy()
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_storage = 0
    
    for index in existing_indexes.keys() :
        if "UNIQUE" in index.upper() : continue
        else : 
            cursor.execute(f"SELECT hypopg_create_index('{index}');")
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            total_storage += cursor.fetchone()[0]/(1024*1024) ## B --> MB
            if res != "" : existing_indexes[index] = indexrelid
    
    conn.commit()
    cursor.close()
    
    return total_storage, existing_indexes

def hypopg_incremental_recommend_creation(conn, recommend_index, current_storage_, storage_constraint, existing_indexes_, workload, schema = 'public') :
    existing_indexes = existing_indexes_.copy()
    constrain_sqls = []
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    current_storage = current_storage_
    recommend_index_ = recommend_index.copy()
    
    # existing indexes
    existed_indexes = parse_index_statements(existing_indexes)
    
    # adjust order of index stats
    recommend_index = [stat for stat in recommend_index_ if stat.lower().strip().startswith('drop')]
    recommend_index.extend([stat for stat in recommend_index_ if stat.lower().strip().startswith('create')])
    
    for index in recommend_index :
        if "create index" in index.lower() :
            # pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(?:USING\s+\w+\s+)?\(([^)]+)\)"
            match = re.search(create_index_regex, index)
            if match :
                index_struct = {"table" : match.group(3), "columns" : [col.strip() for col in match.group(5).split(",")]}
                if index_struct not in existed_indexes.values() :
                    index_name = match.group(1)
                    if index_name in list(existed_indexes.keys()) : index = index.replace(index_name, f"{index_name}_")
                    hypopg_creation = f"SELECT hypopg_create_index('{index}');"
                    try :
                        cursor.execute(hypopg_creation)
                    except psycopg2.Error as e:
                        logger.error(f"Error in sql statement : SELECT hypopg_create_index('{index}');")
                        logger.error(e)
                        conn.commit()
                        cursor.close()
                        cursor = conn.cursor()
                        continue
                    res = cursor.fetchone()[0]
                    # tmp = query_plan_cost_estimation(workload, conn)
                    # logger.info(f"[Incremental Index Creation] ** cost --> {tmp}")
                    indexrelid = int(res.split(',')[0][1:])
                    get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
                    cursor.execute(get_storage_sql)
                    index_size = cursor.fetchone()[0]/(1024*1024) ## B --> MB
                    if current_storage + index_size > storage_constraint :
                        logger.info(f"* there is an index exceeding the storage constraint --> {index}")
                        cursor.execute(f"select hypopg_drop_index({indexrelid});")
                        continue
                    current_storage += index_size 
                    existing_indexes[index] = indexrelid
                    constrain_sqls.append(index)
        elif "drop index" in index.lower() :
            logger.info("drop hypopg index ...")
            index_name = index.split("INDEX")[1].split(";")[0].strip()
            indexrelid = 0
            for ex_index in existing_indexes.keys() :
                if index_name in ex_index : 
                    indexrelid = existing_indexes[ex_index]
                    break
            # logger.debug(f"* the indexrelid is {indexrelid}")
            if indexrelid != 0 :
                get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
                cursor.execute(get_storage_sql)
                index_size = cursor.fetchone()[0]/(1024*1024)
                current_storage -= index_size
                try :
                    cursor.execute(f"select hypopg_drop_index({indexrelid});")
                except psycopg2.Error as e:
                    logger.error(f"Error in sql statement : {index}")
                    logger.error(e)
                    conn.commit()
                    cursor.close()
                    cursor = conn.cursor()
                    current_storage += index_size
                    continue
                constrain_sqls.append(index)
                # tmp = query_plan_cost_estimation(workload, conn)
                # logger.info(f"[Incremental Index Drop] ** cost --> {tmp}")
                reversed_dict = {v: k for k, v in existing_indexes.items()}
                existing_indexes.pop(reversed_dict[indexrelid])
        else :
            logger.error(f"Error index statement : {index}")
            
    cursor.execute("select * from hypopg_list_indexes ;")
    res = cursor.fetchall()
    # logger.debug(f"* [After incremental creation] current hypopg indexes --> {res, len(res)}")
    
    conn.commit()
    cursor.close()
    
    return constrain_sqls, existing_indexes
   
def hypopg_update_existing_indexes(conn, best_indexes, existing_indexes_, current_storage_, schema = 'public') :
    existing_indexes = existing_indexes_.copy()
    current_storage = current_storage_
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    
    cursor.execute("select * from hypopg_list_indexes ;")
    logger.debug(f"* [before best indexes creation] existing indexes length --> {len(existing_indexes)}") # {existing_indexes}, 
    res = cursor.fetchall()
    logger.debug(f"* current hypopg indexes length --> {len(res)}") # {res}, 
    
    for index in best_indexes :
        if "create index" in index.lower() :
            hypopg_creation = f"SELECT hypopg_create_index('{index}');"
            cursor.execute(hypopg_creation)
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            existing_indexes[index] = indexrelid
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            current_storage += cursor.fetchone()[0]/(1024*1024)
        elif "drop index" in index.lower() :
            index_name = index.split("INDEX")[1].split(";")[0].strip()
            indexrelid = 0
            for ex_index in existing_indexes.keys() :
                if index_name in ex_index : 
                    indexrelid = existing_indexes.pop(ex_index)
                    break
            if indexrelid != 0 :
                logger.debug(f"* the indexrelid is {indexrelid}")
                get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
                cursor.execute(get_storage_sql)
                current_storage -= cursor.fetchone()[0]/(1024*1024)
                cursor.execute(f"select hypopg_drop_index({indexrelid});")
            else :
                logger.error(f"* error index statement : {index}")
                if len(index.split(" ")) == 3 :
                    exit()
    
    cursor.execute("select * from hypopg_list_indexes ;")
    res = cursor.fetchall()
    logger.debug(f"*  [After best indexes creation]  current hypopg indexes length --> {len(res)}") # {res}, 
    
    conn.commit()
    cursor.close()
    
    return existing_indexes, current_storage

def hypopg_update_used_indexes(conn, used_indexes, existing_indexes_, best_indexes, schema = 'public') :
    current_storage = 0
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    
    # logger.info("===========================================")
    # logger.info(f"* used_indexes --> {used_indexes}")
    # logger.info(f"* existing_indexes_ --> {existing_indexes_}")
    # logger.info(f"* best_indexes --> {best_indexes}")
    ## drop all hypopg indexes
    hypopg_drop_indexes(conn, schema)
    
    ## keep primary key
    existing_indexes = {}
    for k, v in existing_indexes_.items() :
        if int(v) == 0 :
            existing_indexes[k] = v
    
    ## find all indexes in used_indexes and hypopg create indexes --> existing indexes
    index_creation_sqls = []
    for index in used_indexes :
        find = False
        # if index has already existed
        if any(index in index_.split() for index_ in existing_indexes.keys()) : continue
        # index not existed
        for index_ in existing_indexes_.keys() :
            if index in index_.split() : 
                index_creation_sqls.append(index_)
                find = True
                break
        if not find :
            for index_ in best_indexes :
                if index in index_.split() :
                    index_creation_sqls.append(index_)
                    find = True
                    break
        if not find :
            print(f"Error that used index {index} is not in any set!")
            exit()
    
    index_creation_sqls = list(set(index_creation_sqls))
    logger.debug(f"** index_creation_sqls --> {index_creation_sqls}")
    ## hypopg create indexes and update storage constraint
    for index in index_creation_sqls :
        if "create index" in index.lower() :
            hypopg_creation = f"SELECT hypopg_create_index('{index}');"
            try : 
                cursor.execute(hypopg_creation)
            except psycopg2.Error as e :
                logger.error(e)
                continue
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            existing_indexes[index] = indexrelid
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            current_storage += cursor.fetchone()[0]/(1024*1024)
        else :
            print(f"Error in the index creation sql : {index}")
    
    # logger.info("===========================================")
    # logger.info(f"* [Updated] existing_indexes_ --> {existing_indexes}")

    conn.commit()
    cursor.close()
    
    return existing_indexes, current_storage
    
def parse_index_name(sql) :
    index_name = ''
    
    parsed_sql = sqlparse.parse(sql)
    if len(parsed_sql) == 1 :
        parsed_sql = parsed_sql[0]
        for token in parsed_sql.tokens :
            if isinstance(token, sqlparse.sql.Identifier) :
                if "create index" in sql.lower() or "create unique index" in sql.lower() :
                    index_name = token.value
                elif "drop index" in sql.lower() :
                    print("DROP")
                else :
                    print(f"Error in sql {sql}")
                    exit()
                break
            
    return index_name
            
def query_plan_get_used_indexes(sqls, conn, existing_indexes, schema = 'public') : 
    used_indexes = set([])
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_cost = 0
    cursor.execute(f"SELECT count(*) FROM hypopg_list_indexes;")
    # logger.info(f"* all hypopg indexes --> {cursor.fetchall()}")
    # logger.debug(f"* hypopg_indexes --> {cursor.fetchall()}")
    
    ## get all query plans and obtain used indexes
    view = False
    for sql in sqls :
        if "create view" in sql : # for TPC-H create view query
            view = True
            sqls = sql.split(';')
            if len(sqls) == 4:
                cursor.execute(sqls[0] + ';')
                sql = sqls[1] + ';' 
                drop_view = sqls[2] + ';'
        try : 
            if sql.lower().split()[0] in ['select', 'insert', 'update', 'delete', 'with'] : 
                cursor.execute(f"explain (format json) {sql}")
                res = cursor.fetchone()[0][0]['Plan']
                
                total_cost += res['Total Cost']
                used_indexes_ = query_plan_get_index_name([res])
                
                used_indexes_names = []
                for uidx in used_indexes_ :
                    if '<' in uidx and '>' in uidx :
                        uidx_id = uidx.split('>')[0].split('<')[-1]
                        index_sql = ''
                        for k, v in existing_indexes.items():
                            if str(v) == uidx_id :
                                index_sql = k
                                break
                        uidx = parse_index_name(index_sql)                
                    used_indexes_names.append(uidx)
                        
                used_indexes = used_indexes.union(used_indexes_names)
            else :
                cursor.execute(sql)
        except psycopg2.Error as e :
            logger.error(f"Error in EXPLAIN --> {sql}")
            conn.rollback()
            continue
        if view :
            cursor.execute(drop_view)
            view = False
    conn.commit()
    cursor.close()
    
    # logger.info(f"* used_indexes --> {used_indexes}, {len(used_indexes)}")
    
    return total_cost, used_indexes
        
def query_plan_get_index_name(plans) :
    index_names = []
    for plan in plans :
        
        if "index scan" in plan.get('Node Type').lower() :
            if plan.get('Index Name') : index_names.append(plan.get('Index Name'))
        if "Plans" in plan :
            index_names += query_plan_get_index_name(plan["Plans"])
    index_names = list(set(index_names))
    
    # index_name for the real index creation | <index_relid>index_type_table_column
    return index_names

# for index creation [not hypopg]
def query_plan_cost_estimation_used_indexes(sqls, conn, schema = 'public') :
    total_cost = 0
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    view = False
    used_indexes = []
    
    for sql in sqls :
        if "create view" in sql : # for TPC-H create view query
            view = True
            sqls = sql.split(';')
            if len(sqls) == 4:
                cursor.execute(sqls[0] + ';')
                sql = sqls[1] + ';' 
                drop_view = sqls[2] + ';'

        if sql.lower().split()[0] in ['select', 'insert', 'update', 'delete', 'with'] : 
            cursor.execute(f"explain (format json) {sql}")
            res = cursor.fetchone()[0][0]
            plan = res['Plan']
            # print(plan)
            total_cost += plan['Total Cost']
            used_indexes_ = query_plan_get_index_name([plan])
            used_indexes = list(set(used_indexes).union(set(used_indexes_)))
        else :
            cursor.execute(sql)
        
        if view :
            cursor.execute(drop_view)
            view = False
    conn.commit()
    cursor.close()
    
    return total_cost, used_indexes

def get_existing_indexes(conn, schema = 'public') :
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    get_index_sql = "select indexdef from pg_indexes where schemaname = 'public';"
    
    cursor.execute(get_index_sql)
    res = cursor.fetchall()
    indexdefs = [r[0] for r in res]
    
    cursor.close()
    
    return indexdefs

def update_index_set(hindexes, uindexes, aindexes) :
    logger.info("** Updating indexes set ... ")
    all_indexes = []
    for uidx in uindexes :
        find = False
        ## default set
        for aidx in aindexes :
            if uidx in aidx and 'drop' not in aidx.lower():
                all_indexes.append(aidx)
                find = True
                break
        if not find : # in hindexes
            for hidx in hindexes :
                if uidx in hidx and 'drop' not in hidx.lower() :
                    all_indexes.append(hidx)
                    find = True
                    break
        if not find :
            if uidx.endswith("_pkey") : continue
            logger.debug(f"Error in find index : {uidx}")
            
    return all_indexes

def explain_analyze_get_used_indexes(sqls, conn, existing_indexes, schema = 'public') :
    used_indexes = set([])
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_cost = 0
    execution_time = 0
    
    ## get all query plans and obtain used indexes
    view = False
    cursor.execute("SET statement_timeout TO 6000000;")
    for sql in sqls :
        if "create view" in sql : # for TPC-H create view query
            view = True
            sqls = sql.split(';')
            if len(sqls) == 4:
                cursor.execute(sqls[0] + ';')
                sql = sqls[1] + ';' 
                drop_view = sqls[2] + ';'
        cursor.execute(f"explain (analyze, format json) {sql}")
        res = cursor.fetchone()[0][0]
        tmp = res['Execution Time'] * 0.001
        execution_time += tmp
        logger.debug(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
        res = res['Plan']
        total_cost += res['Total Cost']
        used_indexes_ = query_plan_get_index_name([res])
        
        used_indexes_names = []
        for uidx in used_indexes_ :
            if '<' in uidx and '>' in uidx :
                uidx_id = uidx.split('>')[0].split('<')[-1]
                index_sql = ''
                for k, v in existing_indexes.items():
                    if str(v) == uidx_id :
                        index_sql = k
                        break
                uidx = parse_index_name(index_sql)                
            used_indexes_names.append(uidx)
                
        used_indexes = used_indexes.union(used_indexes_names)
        if view :
            cursor.execute(drop_view)
            view = False
    conn.commit()
    cursor.close()
    
    # logger.debug(f"* used_indexes --> {used_indexes}")
    
    return total_cost, execution_time, used_indexes

def explain_analyze_get_used_indexes_(sqls, conn, schema = 'public') :
    used_indexes = set([])
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_cost = 0
    execution_time = 0
    
    ## get all query plans and obtain used indexes
    view = False
    cursor.execute("SET statement_timeout TO 60000000;")
    for sql in sqls :
        # logger.debug(f"current sql --> {sql}")
        if "create view" in sql : # for TPC-H create view query
            view = True
            sqls = sql.split(';')
            if len(sqls) == 4:
                cursor.execute(sqls[0] + ';')
                sql = sqls[1] + ';' 
                drop_view = sqls[2] + ';'
        
        try :
            cursor.execute(f"explain (analyze, format json) {sql}")
            res = cursor.fetchone()[0][0]
            tmp = res['Execution Time'] * 0.001
            execution_time += tmp
            print(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
            # logger.debug(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
            res = res['Plan']
            total_cost += res['Total Cost']
            # logger.debug(f"* current sql cost --> {res['Total Cost']}")
            used_indexes_ = query_plan_get_index_name([res])
            # logger.debug(f"* current sql used indexes --> {used_indexes_}\n")
        except psycopg2.Error as e :
            logger.debug(f"Error --> {e}")
            conn.rollback()
            execution_time += 60000
            if sql.lower().split()[0] in ['select', 'insert', 'update', 'delete', 'with'] : 
                cursor.execute(f"explain (format json) {sql}")
                res = cursor.fetchone()[0][0]
                res = res['Plan']
                total_cost += res['Total Cost']
                used_indexes_ = query_plan_get_index_name([res])
            else :
                cursor.execute(sql)
                used_indexes_ = []
                
        used_indexes = used_indexes.union(used_indexes_)

        if view :
            cursor.execute(drop_view)
            view = False
    conn.commit()
    cursor.close()
    
    logger.debug(f"* used_indexes --> {used_indexes}")
    
    return total_cost, execution_time, used_indexes

## for actual execution
def incremental_recommend_creation(conn, recommend_index, current_storage_, storage_constraint, existing_indexes_, schema = 'public') :
    existing_indexes = existing_indexes_.copy()
    constrain_sqls = []
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    current_storage = current_storage_
    
    logger.debug(f"[incremental_recommend_creation] existing_indexes --> {existing_indexes}")
    logger.debug(f"[incremental_recommend_creation] recommend_index --> {recommend_index}")
    
    # existing indexes
    existed_indexes = parse_index_statements(existing_indexes)
    
    for index in recommend_index :
        if "create index" in index.lower() :
            hypopg_creation = f"SELECT hypopg_create_index('{index}');"
            try :
                cursor.execute(hypopg_creation)
            except psycopg2.Error as e:
                logger.error(f"Error in sql statement : SELECT hypopg_create_index('{index}');")
                logger.error(e)
                conn.commit()
                cursor.close()
                cursor = conn.cursor()
                continue
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            index_size = cursor.fetchone()[0]/(1024*1024) ## B --> MB
            if current_storage + index_size > storage_constraint :
                logger.info(f"* there is an index exceeding the storage constraint --> {index}")
                cursor.execute(f"select hypopg_drop_index({indexrelid});")
                continue
            
            pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(?:USING\s+\w+\s+)?\(([^)]+)\)"
            match = re.search(pattern, index, re.IGNORECASE)
            if match :
                index_struct = {"table" : match.group(2), "columns" : [col.strip() for col in match.group(3).split(",")]}
                if index_struct not in existed_indexes.values() : # current index has not been created
                    index_name = match.group(1)
                    if index_name in list(existed_indexes.keys()) : index = index.replace(index_name, f"{index_name}_")
                    try :
                        cursor.execute(index)
                    except psycopg2.OperationalError as error :
                        logger.debug(f"* Error in sql {index} --> {error}")
                        conn.rollback()
                        continue
                    current_storage += index_size 
                    existing_indexes[index] = indexrelid
                    constrain_sqls.append(index)
            
        elif "drop index" in index.lower() :
            index_name = index.split("INDEX")[1].split(";")[0].strip()
            indexrelid = 0
            for ex_index in existing_indexes.keys() :
                if index_name in ex_index : 
                    indexrelid = existing_indexes[ex_index]
                    break
            # logger.debug(f"* the indexrelid is {indexrelid}")
            if indexrelid != 0 :
                get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
                cursor.execute(get_storage_sql)
                index_size = cursor.fetchone()[0]/(1024*1024)
                current_storage -= index_size
                try :
                    cursor.execute(f"select hypopg_drop_index({indexrelid});")
                    cursor.execute(index)
                except psycopg2.Error as e:
                    logger.error(f"Error in sql statement : {index}")
                    logger.error(e)
                    conn.commit()
                    cursor.close()
                    cursor = conn.cursor()
                    current_storage += index_size
                    continue
                constrain_sqls.append(index)
                reversed_dict = {v: k for k, v in existing_indexes.items()}
                existing_indexes.pop(reversed_dict[indexrelid])
        else :
            logger.error(f"Error index statement : {index}")
            
    cursor.execute("select * from hypopg_list_indexes ;")
    res = cursor.fetchall()
    # logger.debug(f"* [After incremental creation] current hypopg indexes --> {res, len(res)}")
    
    conn.commit()
    cursor.close()
    
    return constrain_sqls, existing_indexes

def drop_indexes(conn, schema = 'public') :
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    cursor.execute("select * from hypopg_list_indexes;")
    hypopg_indexes = cursor.fetchall()
    for index in hypopg_indexes :
        cursor.execute(f"select hypopg_drop_index({index[0]});")
        
    drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
    drop_index_prefix(conn, drop_index_sql)
    
    conn.commit()
    cursor.close()   
    
def create_existing_indexes(conn, existing_indexes_, schema = 'public') :
    existing_indexes = existing_indexes_.copy()
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_storage = 0
    
    for index in existing_indexes.keys() :
        if "UNIQUE" in index.upper() : continue
        else : 
            cursor.execute(index)
            cursor.execute(f"SELECT hypopg_create_index('{index}');")
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            total_storage += cursor.fetchone()[0]/(1024*1024) ## B --> MB
            if res != "" : existing_indexes[index] = indexrelid
    
    conn.commit()
    cursor.close()
    
    return total_storage, existing_indexes

def update_used_indexes(conn, used_indexes, existing_indexes_, best_indexes, schema = 'public') :
    current_storage = 0
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    
    # logger.debug("===========================================")
    # logger.debug(f"[update_used_indexes] used_indexes --> {used_indexes}")
    # logger.debug(f"[update_used_indexes] existing_indexes_ --> {existing_indexes_}")
    # logger.debug(f"[update_used_indexes] best_indexes --> {best_indexes}")
    ## drop all hypopg indexes
    drop_indexes(conn)
    
    ## keep primary key
    existing_indexes = {}
    for k, v in existing_indexes_.items() :
        if int(v) == 0 :
            existing_indexes[k] = v
    
    ## find all indexes in used_indexes and hypopg create indexes --> existing indexes
    index_creation_sqls = []
    for index in used_indexes :
        find = False
        # if index has already existed
        if any(index in index_.split() for index_ in existing_indexes.keys()) : continue
        # index not existed
        for index_ in existing_indexes_.keys() :
            if index in index_.split() : 
                index_creation_sqls.append(index_)
                find = True
                break
        if not find :
            for index_ in best_indexes :
                if index in index_.split():
                    index_creation_sqls.append(index_)
                    find = True
                    break
        if not find :
            print(f"Error that used index {index} is not in any set!")
            exit()
    
    index_creation_sqls = list(set(index_creation_sqls))
    # logger.debug(f"** index_creation_sqls --> {index_creation_sqls}")
    ## hypopg create indexes and update storage constraint
    for index in index_creation_sqls :
        if "create index" in index.lower() :
            hypopg_creation = f"SELECT hypopg_create_index('{index}');"
            cursor.execute(index)
            cursor.execute(hypopg_creation)
            res = cursor.fetchone()[0]
            indexrelid = int(res.split(',')[0][1:])
            existing_indexes[index] = indexrelid
            get_storage_sql = f"select hypopg_relation_size({indexrelid}) from hypopg_list_indexes where indexrelid = {indexrelid};"
            cursor.execute(get_storage_sql)
            current_storage += cursor.fetchone()[0]/(1024*1024)
        else :
            print(f"Error in the index creation sql : {index}")
    
    conn.commit()
    cursor.close()
    
    return existing_indexes, current_storage

def parse_index_statements(existing_indexes : dict) :
    indexes = {}
    existing_index_statements = existing_indexes.keys()
    
    pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(?:USING\s+\w+\s+)?\(([^)]+)\)"
    
    for ex_index in existing_index_statements :
        match = re.search(pattern, ex_index, re.IGNORECASE)
        if match :
            index_name = match.group(1)
            table_name = match.group(2)
            columns = [col.strip() for col in match.group(3).split(",")]
            indexes[index_name] = {"table" : table_name, "columns" : columns}
        else :
            print(f"* Error index statment : {ex_index}")
            exit()
            
    return indexes
   
## demos
def calculate_similarity(list_a, list_b):
    return sum(1 for x, y in zip(list_a, list_b) if x == y)

def calculate_cosine_similarity(list_a, list_b) :
    A = np.array(list_a)
    B = np.array(list_b)
    
    if A.ndim == 1:
        A = A[:, np.newaxis]  
    if B.ndim == 1:
        B = B[:, np.newaxis]
    
    min_rows = min(A.shape[0], B.shape[0])
    min_cols = min(A.shape[1], B.shape[1])

    A_truncated = A[:min_rows, :min_cols]
    B_truncated = B[:min_rows, :min_cols]       
    
    cosine_similarity = np.dot(A_truncated.flatten(), B_truncated.flatten()) / ( np.linalg.norm(A_truncated) * np.linalg.norm(B_truncated))
    
    return cosine_similarity
 
def demos_cal_meta_data_rows(input_info) :
    demos_meta_data = {}
    
    freq_ndvs_info = sorted([(i['Counts'], i['NDVs'], i['Rows']) for i in input_info['sorted_used_column_cnts_ndvs'].values()], key = lambda x : (-x[0], -x[1], -x[2]))
    
    c_min_val = min(x[0] for x in freq_ndvs_info)
    c_max_val = max(x[0] for x in freq_ndvs_info)
    r_min_val = min(x[2] for x in freq_ndvs_info)
    r_max_val = max(x[2] for x in freq_ndvs_info)
    if c_max_val - c_min_val != 0 and r_max_val - r_min_val != 0 : normalized_freq_ndvs_list = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1], (x[2] - r_min_val) / (r_max_val - r_min_val)) for x in freq_ndvs_info]
    elif c_max_val - c_min_val == 0 : normalized_freq_ndvs_list = [((x[0] - c_min_val), x[1], (x[2] - r_min_val) / (r_max_val - r_min_val)) for x in freq_ndvs_info]
    elif r_max_val - r_min_val == 0 : normalized_freq_ndvs_list = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1], (x[2] - r_min_val)) for x in freq_ndvs_info]
    else : normalized_freq_ndvs_list = [((x[0] - c_min_val), x[1], (x[2] - r_min_val)) for x in freq_ndvs_info]
    
    one_cnts_ndvs_feats = [i[0]*i[1]*i[2] for i in normalized_freq_ndvs_list]
    product_min = min(one_cnts_ndvs_feats)
    product_max = max(one_cnts_ndvs_feats)
    if product_max - product_min != 0 : normalized_one_cnts_ndvs_feats = [(i - product_min) / (product_max - product_min) for i in one_cnts_ndvs_feats]
    else : normalized_one_cnts_ndvs_feats = [(i - product_min) for i in one_cnts_ndvs_feats]
    
    demos_meta_data = {'used_cols' : normalized_freq_ndvs_list, 'used_cols_product' : normalized_one_cnts_ndvs_feats}
    
    # join_ndvs
    join_ndvs_info = sorted([(i[1], input_info['sorted_used_column_cnts_ndvs'][i[0]]['NDVs'], input_info['sorted_used_column_cnts_ndvs'][i[0]]['Rows']) for i in input_info['sorted_other_predicates'].items() if i[0] in input_info['sorted_used_column_cnts_ndvs'].keys()], key = lambda x : (-x[0], -x[1], -x[2]))
    
    c_min_val = min(x[0] for x in join_ndvs_info)
    c_max_val = max(x[0] for x in join_ndvs_info)
    r_min_val = min(x[2] for x in join_ndvs_info)
    r_max_val = max(x[2] for x in join_ndvs_info)
    if c_max_val - c_min_val != 0 and r_max_val - r_min_val != 0 : normalized_join_ndvs_info = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1], (x[2] - r_min_val) / (r_max_val - r_min_val)) for x in join_ndvs_info]
    elif c_max_val - c_min_val == 0 : normalized_join_ndvs_info = [((x[0] - c_min_val), x[1], (x[2] - r_min_val) / (r_max_val - r_min_val)) for x in join_ndvs_info]
    elif r_max_val - r_min_val == 0 : normalized_join_ndvs_info = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1], (x[2] - r_min_val)) for x in join_ndvs_info]
    else : normalized_join_ndvs_info = [( (x[0] - c_min_val), x[1], (x[2] - r_min_val)) for x in join_ndvs_info]
    
    one_join_ndvs_feats = [i[0]*i[1]*i[2] for i in normalized_join_ndvs_info]
    product_min = min(one_join_ndvs_feats)
    product_max = max(one_join_ndvs_feats)
    if product_max - product_min != 0 : normalized_one_join_ndvs_feats = [(i - product_min) / (product_max - product_min) for i in one_join_ndvs_feats]
    else : normalized_one_join_ndvs_feats = [(i - product_min) for i in one_join_ndvs_feats]
    
    demos_meta_data.update({'join_ndvs':normalized_join_ndvs_info, 'join_ndvs_product' : normalized_one_join_ndvs_feats})

    
    return demos_meta_data

def demos_cal_meta_data(input_info) :
    demos_meta_data = {}
    
    freq_ndvs_info = sorted([(i['Counts'], i['NDVs']) for i in input_info['sorted_used_column_cnts_ndvs'].values()], key = lambda x : (-x[0], -x[1]))

    c_min_val = min(x[0] for x in freq_ndvs_info)
    c_max_val = max(x[0] for x in freq_ndvs_info)
    if c_max_val - c_min_val != 0 : normalized_freq_ndvs_list = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1]) for x in freq_ndvs_info]
    else: normalized_freq_ndvs_list = [((x[0] - c_min_val), x[1]) for x in freq_ndvs_info]
    
    one_cnts_ndvs_feats = [i[0]*i[1] for i in normalized_freq_ndvs_list]
    product_min = min(one_cnts_ndvs_feats)
    product_max = max(one_cnts_ndvs_feats)
    if product_max - product_min != 0 : normalized_one_cnts_ndvs_feats = [(i - product_min) / (product_max - product_min) for i in one_cnts_ndvs_feats]
    else : normalized_one_cnts_ndvs_feats = [(i - product_min) for i in one_cnts_ndvs_feats]
    
    demos_meta_data = {'used_cols' : normalized_freq_ndvs_list, 'used_cols_product' : normalized_one_cnts_ndvs_feats}
    
    # join_ndvs
    if input_info['sorted_other_predicates'] != {} :
        join_ndvs_info = sorted([(i[1], input_info['sorted_used_column_cnts_ndvs'][i[0]]['NDVs']) for i in input_info['sorted_other_predicates'].items() if i[0] in input_info['sorted_used_column_cnts_ndvs'].keys()], key = lambda x : (-x[0], -x[1]))
        
        c_min_val = min(x[0] for x in join_ndvs_info)
        c_max_val = max(x[0] for x in join_ndvs_info)
        if c_max_val - c_min_val != 0 : normalized_join_ndvs_info = [((x[0] - c_min_val) / (c_max_val - c_min_val), x[1]) for x in join_ndvs_info]
        else : normalized_join_ndvs_info = [( (x[0] - c_min_val), x[1]) for x in join_ndvs_info]
        
        one_join_ndvs_feats = [i[0]*i[1] for i in normalized_join_ndvs_info]
        product_min = min(one_join_ndvs_feats)
        product_max = max(one_join_ndvs_feats)
        if product_max - product_min != 0 : normalized_one_join_ndvs_feats = [(i - product_min) / (product_max - product_min) for i in one_join_ndvs_feats]
        else : normalized_one_join_ndvs_feats = [(i - product_min) for i in one_join_ndvs_feats]
        
        demos_meta_data.update({'join_ndvs':normalized_join_ndvs_info, 'join_ndvs_product' : normalized_one_join_ndvs_feats})
    else : 
        demos_meta_data.update({'join_ndvs':[], 'join_ndvs_product' : []})
    
    return demos_meta_data

def demos_match(input_info, recommend_demos, iter_idx, num, args, feat = 0, top_n = 6) : 
    logger.info(f"Demonstration Matching [{feat}]...")
    demo_ids = []
    
    demos_path = args["demos_path"]
    demos_meta_data_path = args["demos_meta_data_path"]
    
    # demos_meta_data
    db_bench = {'imdb' : 'job', 'tpch_5' : 'tpch', 'tpcds_1' : 'tpcds'}
    key = ""
    for db_b in db_bench.values() :
        if db_b in demos_path :
            key = db_b
            break
    demos_meta_data = {} # demos_idx -> demos_feat {where, join}
    # calculate columns set in demos
    if os.path.exists(demos_meta_data_path) :
        with open(demos_meta_data_path) as file :
            demos_meta_data = json.load(file)
    else : # feature
        if not os.path.exists(os.path.dirname(demos_meta_data_path)) :
            os.makedirs(os.path.dirname(demos_meta_data_path))
        # used_columns, join_columns [both are sorted via freq]
        for demo_id, demo in recommend_demos.items() :
            if 'cross' not in demos_meta_data_path : # feature with schema
                sorted_used_column_cnts_ndvs_cols = list(demo['sorted_used_column_cnts_ndvs'].keys())
                sorted_other_predicates_cols = list(demo['sorted_other_predicates'].keys())
                demos_meta_data[demo_id] = {'used_columns' : sorted_used_column_cnts_ndvs_cols, 'join_predicates' : sorted_other_predicates_cols}
            else : # feature w/o schema
                demos_meta_data[demo_id] = demos_cal_meta_data(demo)
                
        with open(demos_meta_data_path, 'w') as file :
            json.dump(demos_meta_data, file, indent=4)
    
    # input info
    input_info_feat = demos_cal_meta_data(input_info)
    # print(input_info_feat)
    
    normalized_freq_ndvs_list_si = [(sublist_id, calculate_cosine_similarity(input_info_feat['used_cols'], sublist['used_cols'])) for sublist_id, sublist in demos_meta_data.items()]
    freqs_ndvs_product_si = [(sublist_id, calculate_cosine_similarity(input_info_feat['used_cols_product'], sublist['used_cols_product'])) for sublist_id, sublist in demos_meta_data.items()]
    
    if input_info_feat['join_ndvs'] != [] :
        join_ndvs_info_si = [(sublist_id, calculate_cosine_similarity(input_info_feat['join_ndvs'], sublist['join_ndvs'])) for sublist_id, sublist in demos_meta_data.items()]
        join_ndvs_product_si = [(sublist_id, calculate_cosine_similarity(input_info_feat['join_ndvs_product'], sublist['join_ndvs_product'])) for sublist_id, sublist in demos_meta_data.items()]
    else : 
        join_ndvs_info_si = []
        join_ndvs_product_si = []
    
    w_1 = 0.5
    w_2 = 0.5
    if join_ndvs_info_si != [] :
        freq_join_si = [(i[0], w_1 * np.array(i[1]) + w_2 * np.array(j[1])) for i, j in zip(normalized_freq_ndvs_list_si, join_ndvs_info_si)]
        freq_join_product_si = [(i[0], w_1 * float(i[1]) + w_2 * float(j[1])) for i, j in zip(freqs_ndvs_product_si, join_ndvs_product_si)]
    else :
        freq_join_si = normalized_freq_ndvs_list_si
        freq_join_product_si = freqs_ndvs_product_si
        
    # logger.info(f"Similarities 0 --> {sorted(normalized_freq_ndvs_list_si, key=lambda x: x[1], reverse=True)}")
    # logger.info(f"Similarities 1 --> {sorted(freqs_ndvs_product_si, key=lambda x: x[1], reverse=True)}")
    # logger.info(f"Similarities 2 --> {sorted(join_ndvs_info_si, key=lambda x: x[1], reverse=True)}")
    # logger.info(f"Similarities 3 --> {sorted(join_ndvs_product_si, key=lambda x: x[1], reverse=True)}")
    # logger.info(f"Similarities 4 --> {sorted(freq_join_si, key=lambda x: x[1], reverse=True)}")
    # logger.info(f"Similarities 5 --> {sorted(freq_join_product_si, key=lambda x: x[1], reverse=True)}")
    # exit()
    
    if feat == 0 :
        similarities = normalized_freq_ndvs_list_si
    elif feat == 1 :
        similarities = freqs_ndvs_product_si
    elif feat == 2 :
        similarities = join_ndvs_info_si
    elif feat == 3 :
        similarities = join_ndvs_product_si
    elif feat == 4 :
        similarities = freq_join_si
    elif feat == 5 :
        similarities = freq_join_product_si
        
    sorted_similarities = sorted(similarities, key=lambda x: x[1], reverse=True)
    # logger.info(f"Similarities --> {sorted_similarities}")
    # print([sorted_si[0] for sorted_si in sorted_similarities][:10])
    # print(sorted_similarities)
    # exit()
    demos_candidates = sorted_similarities[:top_n]

    if iter_idx <= 1 :
        demo_ids = [ss[0] for ss in sorted_similarities[:num]]   
    else :
        demo_ids = [ss[0] for ss in sorted_similarities[(iter_idx-1) * num : (iter_idx-1) * num + num]] 
    
    # demos_candidates = recommend_demos
    # demo_ids = random.sample(list(demos_candidates.keys()), num)
    
    return demo_ids

def demos_match_cluster(input_info, recommend_demos, iter_idx, num, args, feat = 0, top_n = 6) : 
    logger.info(f"Demonstration Matching [{feat}]...")
    demo_ids = []
    
    demos_meta_data_path = args["demos_meta_data_path"]
    
    demos_meta_data = {} # demos_idx -> demos_feat {where, join}
    # calculate columns set in demos
    if os.path.exists(demos_meta_data_path) :
        with open(demos_meta_data_path) as file :
            demos_meta_data = json.load(file)       
    else : # feature
        if not os.path.exists(os.path.dirname(demos_meta_data_path)) :
            os.makedirs(os.path.dirname(demos_meta_data_path))
        # used_columns, join_columns [both are sorted via freq]
        for demo_id, demo in recommend_demos.items() :
            if 'cross' not in demos_meta_data_path : # feature with schema
                sorted_used_column_cnts_ndvs_cols = list(demo['sorted_used_column_cnts_ndvs'].keys())
                sorted_other_predicates_cols = list(demo['sorted_other_predicates'].keys())
                demos_meta_data[demo_id] = {'used_columns' : sorted_used_column_cnts_ndvs_cols, 'join_predicates' : sorted_other_predicates_cols}
            else : # feature w/o schema
                demos_meta_data[demo_id] = demos_cal_meta_data(demo)
                
        with open(demos_meta_data_path, 'w') as file :
            json.dump(demos_meta_data, file, indent=4)
    
    # input info
    input_info_feat = demos_cal_meta_data(input_info)
    
    used_cols_ori_data = [(k, v['used_cols']) for k, v in demos_meta_data.items()]
    max_length = max([len(item[1]) for item in used_cols_ori_data])
    used_cols_meta_data = [(item[0], np.pad(np.array(item[1]), ((0, max_length - len(item[1])),(0,0)), constant_values=0)) for item in used_cols_ori_data]

    used_cols_p_ori_data = [(k, v['used_cols_product']) for k, v in demos_meta_data.items()]
    max_length = max([len(item[1]) for item in used_cols_p_ori_data])
    used_cols_p_meta_data = [(item[0], np.pad(item[1], (0, max_length - len(item[1])), constant_values=0)) for item in used_cols_p_ori_data]
    
    join_ndvs_ori_data = [(k, v['join_ndvs']) for k, v in demos_meta_data.items()]
    max_length = max([len(item[1]) for item in join_ndvs_ori_data])
    join_ndvs_meta_data = [(item[0], np.pad(np.array(item[1]), ((0, max_length - len(item[1])),(0,0)), constant_values=0)) for item in join_ndvs_ori_data]
    
    join_ndvs_ori_data = [(k, v['join_ndvs_product']) for k, v in demos_meta_data.items()]
    max_length = max([len(item[1]) for item in join_ndvs_ori_data])
    join_ndvs_p_meta_data = [(item[0], np.pad(item[1], (0, max_length - len(item[1])), constant_values=0)) for item in join_ndvs_ori_data]
    
    # cluster
    num_clusters = int(len(demos_meta_data)/20)
    kmeans = KMeans(n_clusters=num_clusters, random_state=0, n_init=10)
    
    if feat == 0 :
        flatten_data = [item[1].flatten() for item in used_cols_meta_data]
        kmeans.fit(flatten_data)
        max_length = max([len(item[1]) for item in used_cols_ori_data])
        
        input_meta_data = input_info_feat['used_cols']
        if len(input_meta_data) >= max_length : input_meta_data = np.array(input_meta_data[:max_length]).flatten().reshape(1, -1)
        else : input_meta_data = np.pad(np.array(input_meta_data), ((0, max_length - len(input_meta_data)),(0,0)), constant_values=0).flatten().reshape(1, -1)
        
        feature_data = used_cols_ori_data
    elif feat == 1 :
        kmeans.fit([item[1] for item in used_cols_p_meta_data])
        max_length = max([len(item[1]) for item in used_cols_p_ori_data])
        
        input_meta_data = input_info_feat['used_cols_product']
        if len(input_meta_data) >= max_length : input_meta_data = np.array(input_meta_data[:max_length]).flatten().reshape(1, -1)
        else : input_meta_data = np.pad(np.array(input_meta_data), ((0, max_length - len(input_meta_data))), constant_values=0).flatten().reshape(1, -1)
        
        feature_data = used_cols_p_meta_data
    elif feat == 2 :
        flatten_data = [item[1].flatten() for item in join_ndvs_meta_data]
        kmeans.fit(flatten_data)
        max_length = max([len(item[1]) for item in join_ndvs_ori_data])
        input_meta_data = input_info_feat['join_ndvs']
        
        if len(input_meta_data) >= max_length : input_meta_data = np.array(input_meta_data[:max_length]).flatten().reshape(1, -1)
        else : input_meta_data = np.pad(np.array(input_meta_data), ((0, max_length - len(input_meta_data)),(0,0)), constant_values=0).flatten().reshape(1, -1)
        
        feature_data = join_ndvs_meta_data
    else : 
        kmeans.fit([item[1] for item in join_ndvs_p_meta_data])
        max_length = max([len(item[1]) for item in join_ndvs_ori_data])
        
        input_meta_data = input_info_feat['join_ndvs_product']
        if len(input_meta_data) >= max_length : input_meta_data = np.array(input_meta_data[:max_length]).flatten().reshape(1, -1)
        else : input_meta_data = np.pad(np.array(input_meta_data), ((0, max_length - len(input_meta_data))), constant_values=0).flatten().reshape(1, -1)
        
        feature_data = join_ndvs_p_meta_data
    
    distances = np.linalg.norm(input_meta_data[:, np.newaxis] - kmeans.cluster_centers_, axis=2)

    # 分配到最近的质心
    cluster_assignments = np.argmin(distances, axis=1)
    list_kmeans_labels = list(kmeans.labels_)
    
    demos_cluster = []
    for i, label in enumerate(list_kmeans_labels) :
        if label in cluster_assignments :
            demos_cluster.append(feature_data[i][0])

    
    demo_ids = random.sample(demos_cluster, num)
    
    return demo_ids

def write_history_cost_str(historical_info, historical_costs, historical_costs_str) :
    assert len(historical_info) == len(historical_costs) and len(historical_costs_str) + 1 == len(historical_info), f"The length of info and costs not match ({len(historical_costs_str)},{len(historical_info)})"
    
    default_cost = historical_costs[0]
    current_cost = historical_costs[-1] 
    
    historical_costs_str_ = historical_costs_str.copy()
    
    if current_cost < default_cost :
        ratio = round((default_cost - current_cost) * 100 / default_cost, 2)
        historical_costs_str_.append(f"reduce {ratio}% than default cost")
    elif current_cost == default_cost :
        historical_costs_str_.append("default cost")
    else :
        ratio = round((current_cost - default_cost) * 100 / default_cost, 2)
        historical_costs_str_.append(f"increase {ratio}% than default cost")
        
    return historical_costs_str_
    
def extract_index_info(index_stats) :
    index_infos = []
    for index_stat in index_stats :
        match = re.fullmatch(create_index_regex, index_stat)
        if match :
            index_name = match.group(2)
            table_name = match.group(3)
            columns_name = [col.strip() for col in match.group(5).split(",")]
            index = index_name + '('
            for column in columns_name :
                index += table_name + '.' + column + ','
            index = index[:-1] + ')'
            index_infos.append(index)
        else :
            print(f"Error index statements {index_stat}")
            exit()
            
    return index_infos

def bucketing(input) :
    assert type(input) == float, f"[Bucketing] Error type of input {input} : {type(input)}"
    
    if input < 0.0001 : return "Negligible"
    elif input >= 0.0001 and input < 0.01 : return "Very Low"
    elif input >= 0.01 and input < 0.1 : return "Low"
    elif input >= 0.1 and input < 0.3 : return "Below Mid"
    elif input >= 0.3 and input < 0.7 : return "Mid"
    else : return "High"

def recom_indexes_estimation(current_storage, storage_constraint, existing_indexes, workload, idx, recom_index, db_name, recommend_tcost, recommend_constraint_sqls, total_used_indexes, schema = 'public') :
    config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = config.user,
        password = config.password,
        host = config.host,
        port = config.port
    ) 
    
    current_storage, existing_indexes = hypopg_create_existing_indexes(conn, existing_indexes, schema)
    
    # logger.debug(f"* the {idx}th recom_index is {recom_index}")
    constraint_sqls, ex_indexes = hypopg_incremental_recommend_creation(conn, recom_index, current_storage, storage_constraint, existing_indexes, workload, schema)
    logger.debug(f"* constraint_sqls --> {constraint_sqls}")
    current_cost, used_indexes = query_plan_get_used_indexes(workload, conn, ex_indexes, schema)
    logger.info(f"* the {idx}th estimated cost is {current_cost}")
    hypopg_drop_indexes(conn, schema)
    
    conn.close()
    
    recommend_tcost[idx] = current_cost
    recommend_constraint_sqls[idx] = constraint_sqls
    total_used_indexes[idx] = used_indexes
        
def multi_process_recom_indexes_estimation(recom_indexes, current_storage, storage_constraint, existing_indexes, workload, db_name, schema = "public", multi_pro = 9) :
    global semaphore 
    semaphore = Semaphore(multi_pro)
    processes = []
    manager = Manager()
    
    multi_pro = len(recom_indexes)
    
    recommend_tcost = manager.list([None] * multi_pro)
    recommend_constraint_sqls = manager.list([None] * multi_pro)
    total_used_indexes = manager.list([None] * multi_pro)
    
    for idx, recom_index in enumerate(recom_indexes) :
        semaphore.acquire() 
        
        process = Process(
            target=recom_indexes_estimation,
            args=(current_storage, storage_constraint, existing_indexes, workload, idx, recom_index, db_name, recommend_tcost, recommend_constraint_sqls, total_used_indexes, schema)
        )
        processes.append(process)
        process.start()
        
    for process in processes:
        process.join()
        
    recommend_tcost = list(recommend_tcost)
    recommend_constraint_sqls = list(recommend_constraint_sqls)
    total_used_indexes = list(total_used_indexes)
    # print(recommend_tcost)
    # exit()
    
    return recommend_tcost, recommend_constraint_sqls, total_used_indexes

def prefix_list(A, B):
    for element in B:
        if len(element) >= len(A) and element[:len(A)] == A:
            return True
    return False