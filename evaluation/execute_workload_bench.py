import psycopg2
import time
import glob
import os
import argparse
import json
import re
import shutil
import statistics
import sys
sys.path.append("..")

import logging
from functions import execute_sql_file, explain_analyze_get_used_indexes_, query_plan_get_index_name
from config import DBConfig

logger = logging.getLogger('log')
logger.setLevel(logging.DEBUG)
    
def parse() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, help="database name", default="tpcds_1")
    parser.add_argument("--workload", type=str, help="workload path", default="./workload/tpcds.sql")
    parser.add_argument("--pattern", type=str, help="index file name pattern", default="index*.sql") 
    parser.add_argument("--directory", type=str, help="index file dir", default="./tpcds")
    parser.add_argument("--output", type=str, help="output file path", default="./results/output.txt")
    
    return parser.parse_args()

def execute_sql_only(conn, sql_file_path) :
     with conn.cursor() as cur:
        with open(sql_file_path, 'r') as sql_file:
            sql_commands = sql_file.readlines()
        sql_commands = [line.strip() for line in sql_commands if line.strip() and not line.strip().startswith('--')]
        
        if len(sql_commands) == 0 :
            logger.info("* there isn't any indexes in this file.")
            return False
        
        for sql in sql_commands:
            try :
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                logger.error(f"Indexes creation failed : {sql}")
                conn.commit()
                # continue

        return True
    
def drop_index_prefix(conn, SQL = "select indexname from pg_indexes where indexname like 'index%';" ) :
    index_name = SQL
    # print(index_name)  
    cur = conn.cursor()
    cur.execute(index_name)
    indexes = cur.fetchall()
    if not indexes:
        logger.warning("No indexes found with the specified prefix.")
        return
    drop_index_queries = [
        f"DROP INDEX IF EXISTS {index[0]};" for index in indexes
    ]
    for drop_query in drop_index_queries:
        cur.execute(drop_query)    
    conn.commit()
    
def rename_files(directory, pat, number = False) :
    filelist = glob.glob(os.path.join(directory, pat))
    
    def extract_number(file_name): 
        match = re.search(r'_(\d+)\.sql$', file_name)
        return int(match.group(1)) if match else float('inf')  

    if number : filelist = sorted(filelist, key = extract_number)
    else : filelist = sorted(filelist)
    
    if not os.path.exists(f"{directory}/rename/") :
        os.makedirs(f"{directory}/rename/")
    
    for i in range(len(filelist)) :
        logger.info(f"file {filelist[i]} rename --> file {directory}/rename/index_{i}.sql")
        shutil.copy(filelist[i],f"{directory}/rename/index_{i}.sql")
        # os.rename(filelist[i],f"{directory}/rename/index_{i}.sql")
    
    directory = f"{directory}/rename/"
    
    return directory
   
def extract_number(filename):
    filename = os.path.basename(filename)
    match = re.search(r'\d+', filename)
    return int(match.group()) if match else float('-inf')

def explain_analyze_get_used_indexes_bar(sqls, conn, bar, schema = 'public') :
    used_indexes = set([])
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema};")
    total_cost = 0
    execution_time = 0
    
    out_of_bar = False
    
    view = False
    cursor.execute("SET statement_timeout TO 60000000;")
    i = 0
    for i, sql in enumerate(sqls) :
        if "create view" in sql : # for TPC-H create view query
            view = True
            view_sqls = sql.split(';')
            if len(view_sqls) == 4:
                cursor.execute(view_sqls[0] + ';')
                sql = view_sqls[1] + ';' 
                drop_view = view_sqls[2] + ';'
        
        try :
            cursor.execute(f"explain (analyze, format json) {sql}")
            res = cursor.fetchone()[0][0]
            tmp = res['Execution Time'] * 0.001
            execution_time += tmp
            print(f"* current sql execution time is {tmp} and now the actual total time is {execution_time}.")
            res = res['Plan']
            total_cost += res['Total Cost']
            used_indexes_ = query_plan_get_index_name([res])
        except psycopg2.Error as e :
            logger.debug(f"Error --> {e}")
            conn.rollback()
            execution_time += 60000
            cursor.execute(f"explain (format json) {sql}")
            res = cursor.fetchone()[0][0]
            res = res['Plan']
            total_cost += res['Total Cost']
            used_indexes_ = query_plan_get_index_name([res])
                
        used_indexes = used_indexes.union(used_indexes_)
        if view :
            cursor.execute(drop_view)
            view = False
            
        if execution_time >= bar : 
            out_of_bar = True
            logger.error(f"[Time out of the Bar] Current SQL index is {i}/{len(sqls)} (0)")
            break
        
    conn.commit()
    cursor.close()
    
    logger.debug(f"* used_indexes --> {used_indexes}")
    
    return total_cost, execution_time, used_indexes, out_of_bar

if __name__ == "__main__" :
    ## parse config_bm.json
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="./config/config_bm_tpch.json")
    config_path = parser.parse_args().config
    args = json.load(open(config_path, 'r'))
    
    ## parameters
    db_name = args["database"]
    workload_path = args["workload_path"]
    pattern = args["pattern"]
    directory_mm = args["index_directory"]
    output_path = args["output_path"]
    drop_default_indexes_of_job = args["drop_default_indexes"]
    number = args["number_sorted"]
    execution_time_n = args["execution_time"]
    rename = args['rename']
    schema = args['schema']

    ## logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    if not os.path.exists(os.path.dirname(output_path)) :
        os.makedirs(os.path.dirname(output_path))
    with open(output_path, "w") as file :
        file.truncate()
    file_handler = logging.FileHandler(output_path, mode = 'a')
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    ## rename file name
    if rename : directory_mm = rename_files(directory_mm, pattern, number)
    
    logger.info(f"** workload test in database {db_name} has begun.")
       
    db_config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = db_config.user,
        password = db_config.password,
        host = db_config.host,
        port = db_config.port
    ) 
    
    ## workload information
    with open(workload_path, 'r') as file :
        sqls = file.readlines()
        logger.info(f"* the num of sqls in this workload is {len(sqls)}")
   
    ## index files information
    filelist = glob.glob(os.path.join(directory_mm, pattern))
    if number : filelist = sorted(filelist, key = extract_number)
    logger.info(f"* The number of index files is {len(filelist)}.")
    logger.info(f"** The paths of index files are as followed >>> {filelist}")
  
    ## initialize the indexes [drop default index of job]
    drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
    if drop_default_indexes_of_job : drop_index_prefix(conn, drop_index_sql)
     
    execution_times = []
    ## test execution
    logger.info(f"with database {db_name} the first execution for test")
    execution_time = execute_sql_file(conn, workload_path, schema)
    logger.info(f"** Test execution time is {execution_time}")
    
    ## first execution with default index settings
    logger.info("********************************* Start! *********************************")
    f_execution_time = 0
    etimes = []
    for i in range(execution_time_n + 1) :
        # t = execute_sql_file(conn, workload_path)
        _, t, used_indexes = explain_analyze_get_used_indexes_(sqls, conn, schema)
        t = round(t, 4)
        if i != 0 :
            logger.debug(f"** the {i-1}th execution time is {t}.")
            etimes.append(t)
            f_execution_time += t
    f_execution_time = f_execution_time / execution_time_n
    f_execution_time = statistics.median(etimes)
    logger.info(f"* default execution time is {f_execution_time}")
    
    ## index creation experiments
    file_names = []
    for idx, path in enumerate(sorted(filelist)):
        logger.info(f"* current index path is {path}")
        drop_index_prefix(conn, drop_index_sql)
        file_names.append(os.path.basename(path))
        is_idx = execute_sql_only(conn, path)  # index creation
        if is_idx :
            logger.info("** Indexes created")
            execution_time = 0
            etimes = []
            for i in range(execution_time_n + 1) :
                _, t, used_indexes, out_of_bar = explain_analyze_get_used_indexes_bar(sqls, conn, f_execution_time, schema)
                t = round(t, 4)
                if i != 0 :
                    if out_of_bar : 
                        etimes.append(t)
                        logger.debug(f"** the {i-1}th execution time is {t} [Out of Default].")
                        break
                    else : 
                        logger.debug(f"** the {i-1}th execution time is {t}.")
                        # execution_time += t
                        etimes.append(t)
            # execution_time = execution_time / execution_time_n
            execution_time = statistics.median(etimes)
            if out_of_bar : logger.info(f"* with index file in {path}, execution_time is {execution_time} [Out of Default]")
            else : logger.info(f"* with index file in {path}, execution_time is {execution_time}")
            execution_times.append(execution_time)
        else :
            if idx == 0 :
                execution_time = 0
                etimes = []
                for i in range(execution_time_n + 1) :
                    _, t, used_indexes, out_of_bar = explain_analyze_get_used_indexes_bar(sqls, conn, f_execution_time, schema)
                    if i != 0 :
                        logger.debug(f"** the {i-1}th execution time is {t}.")
                        execution_time += t
                        etimes.append(t)
                execution_time = execution_time / execution_time_n
                execution_time = statistics.median(etimes)
                logger.info(f"* with index file in {path}, execution_time is {execution_time}")
                execution_times.append(execution_time)
   
    # execution_times, f_execution_time           
    radios = []
    radios_ = []
    for execution_time in execution_times :
        radio = round((f_execution_time - execution_time) / f_execution_time, 4)
        radios.append(f"{radio * 100}% ↓")
        radios_.append(radio * 100)
     
    logger.info(f"** workload test has finished.\n")
    logger.info(f"** Default execution times --> {f_execution_time}.\n")
    logger.info(f"** Execution times --> {execution_times}.\n")
    logger.info(f"** Radios --> {radios}.\n")
    logger.info(f"** Radios_ --> {radios_}.\n")
    logger.info(f"** workload test in database {db_name} has finished.")
    
    with open(output_path, 'a') as file :
        file.write("\n**********************************************************\n")
        for file_name, radio in zip(file_names, radios_) :
            file.write(f"{file_name} : {radio}\n")