# import mysql.connector
import psycopg2
import os
import re
import sqlparse
import random
import json
import subprocess

from config import DBConfig

def check_sql_validity(sql, db_name):
    conn = psycopg2.connect(
        host = DBConfig.host,
        user = DBConfig.user,
        password = DBConfig.password,
        port = DBConfig.port,
        dbname = db_name
    )
    
    sql = sql.strip()
    if sql[-1] != ";":
        sql = sql + ";"
    
    sql = "BEGIN;\nEXPLAIN {}\nROLLBACK;".format(sql)

    execution_error = None
    try:
        cur = conn.cursor()
        cur.execute(sql)
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
        execution_error = str(e)
    finally:
        cur.close()
        conn.close()
    
    return execution_error

def execute_sql(sql, db_name):
    conn = psycopg2.connect(
        host = DBConfig.host,
        user = DBConfig.user,
        password = DBConfig.password,
        port = DBConfig.port,
        dbname = db_name
    )

    results = None
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout TO 360000;")
        cur.execute(sql)
        results = cur.fetchall()
        # print(results)
    except psycopg2.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()
    
    return results

def get_all_db_names():
    db_name = "postgres"
    sql = "SELECT datname FROM pg_database;"
    results = execute_sql(sql, db_name)

    if results is not None:
        databases = [res[0] for res in results]
    else:
        databases = []
    
    return databases

def get_table_names_from_db(db_name):
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    results = execute_sql(sql, db_name)

    if results is not None:
        table_names = [res[0] for res in results]
    else:
        table_names = []
    
    return table_names

def get_column_names_from_table(db_name, table_name):
    sql = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}';".format(table_name)
    results = execute_sql(sql, db_name)

    if results is not None:
        column_names = [res[0] for res in results]
    else:
        column_names = []

    return column_names

def get_db_info(db_name, table_names):
    # get information each table (including CREATE TABLE, FOREIGN KEY, PRIMARY KEY, etc.)
    create_table_statements, primary_key_constraints, foreign_key_constraints, create_index_statements = [], [], [], []
    for table_name in table_names:
        os.environ['PGPASSWORD'] = DBConfig.password
        command = 'pg_dump -s -h {} -p {} -U {} -d {} -t "{}" --schema-only'.format(
            DBConfig.host,
            DBConfig.port,
            DBConfig.user,
            db_name,
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

def format_stmt(stmt):
    stmt = stmt.replace("\n", " ")
    stmt = stmt.replace("\t", " ")
    
    while "  " in stmt:
        stmt = stmt.replace("  ", " ")
    stmt = sqlparse.format(stmt, keyword_case = "upper", identifier_case = "lower")

    return stmt.strip()

def get_rows(db_name, table_name, row_num):
    all_column_names = get_column_names_from_table(db_name, table_name)
    # print(column_names)
    sql = 'SELECT {} FROM {} LIMIT {};'.format(", ".join([cn for cn in all_column_names]), table_name, row_num)
    results = execute_sql(sql, db_name)

    return sql, results

def get_db_prompt(db_name, max_query_num):
    ddl_statements = json.load(open(os.path.join("data/databases", db_name, "{}_ddl.json".format(db_name))))
    ddl_statements = [format_stmt(stmt) for stmt in ddl_statements]

    if max_query_num == -1:
        select_statements = []
        select_statement_num = random.randint(5, 30)
    else:
        select_statements = json.load(open(os.path.join("data/databases", db_name, "{}_queries.json".format(db_name))))
        select_statements = [format_stmt(stmt) for stmt in select_statements]
        
        if len(select_statements) >= max_query_num:
            if max_query_num < 2:
                raise ValueError("max_query_num >= 2 is required.")
            select_statements = random.sample(select_statements, random.randint(2, max_query_num))
        else:
            random.shuffle(select_statements)
        select_statement_num = len(select_statements)

    database_seq = "The statements below detail the architectural design of the {} database:\n".format(db_name)
    database_seq = "DATABASE DDL STATEMENTS:\n" + "\n".join(ddl_statements) + "\n"    
    database_seq += "Building upon the previously mentioned database statements, we have a workload comprising {} complex SQL queries:\n".format(select_statement_num)
    
    if len(select_statements) > 0:
        select_statements_dict = {}
        for idx, select_statement in enumerate(select_statements):
            select_statements_dict["query{}".format(idx)] = select_statement
        select_statement_seq = json.dumps(select_statements_dict, indent = 2, ensure_ascii = False)
    else:
        select_statement_seq = ""

    return database_seq, select_statement_seq

def get_db_information(db_name):
    pass

def count_tp_nums_json(db_name, path) :
    table_names = get_table_names_from_db(db_name)
    results = []
    for table in table_names :
        sql = f"SELECT COUNT(*) FROM {table};"
        result = execute_sql(sql, db_name)[0][0]
        results.append(result)
    db_tp_nums = dict(zip(table_names, results))
    # tp_nums = {}
    # tp_nums[db_name] = db_tp_nums
    with open(path, "w") as file :
        json.dump(db_tp_nums, file, indent = 4)
    return db_tp_nums

if __name__ == "__main__":
    # get_all_db_names()
    get_table_names_from_db("lzz")
    # print(execute_sql(sql = "SELECT * FROM supplier ORDER BY RANDOM() LIMIT 2;", dbname = "lzz"))