import psycopg2
import time
import glob
import os
import argparse
import sys
import json
import sys
sys.path.append("..")

import logging
from config import DBConfig
from functions import hypopg_indexes_creation_constraint, query_plan_cost_estimation, hypopg_drop_indexes
from functions import oltp_stress_test_db

def parse() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, help="database name", default="tpcds_1")
    parser.add_argument("--workload", type=str, help="workload path", default="./workload/tpcds.sql")
    parser.add_argument("--pattern", type=str, help="index file name pattern", default="index*.sql") 
    parser.add_argument("--directory", type=str, help="index file dir", default="./tpcds")
    parser.add_argument("--output", type=str, help="output file path", default="./results/output.txt")
    
    return parser.parse_args()

def execute_sql_only(conn, sql_file_path, schema = 'public') :
     with conn.cursor() as cur:
        with open(sql_file_path, 'r') as sql_file:
            sql_commands = sql_file.readlines()
        sql_commands = [line.strip() for line in sql_commands if line.strip() and not line.strip().startswith('--')]
        
        for sql in sql_commands:
            try :
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                logger.error(f"Indexes creation failed : {sql}")
                conn.commit()

def drop_index_prefix(conn, SQL = "select indexname from pg_indexes where indexname like 'index%';" ) :
    index_name = SQL
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
    
def rename_mm(directory, pat) :
    filelist = glob.glob(os.path.join(directory, pat))
    filelist = sorted(filelist)
    for i in range(len(filelist)) :
        os.rename(filelist[i],f"{directory}/index_{i}.sql")
    
if __name__ == "__main__" :
    ## parse config_bm.json
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="./config_bm.json")
    config_path = parser.parse_args().config
    args = json.load(open(config_path, 'r'))
    
    ## parameters
    db_name = args["database"]
    workload_path = args["workload_path"]
    pattern = args["pattern"]
    directory_mm = args["index_directory"]
    output_path = args["output_path"]
    drop_default_indexes_of_job = args["drop_default_indexes"]
    index_storage_proportion = args["index_storage_proportion"]
    schema = args['schema']
    
    ## logging
    logger = logging.getLogger('log')
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    if not os.path.exists(os.path.dirname(output_path)) :
        os.makedirs(os.path.dirname(output_path))
    with open(output_path, "w") as file :
        file.truncate()
    file_handler = logging.FileHandler(output_path, mode = 'a')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.info(f"** workload test in database {db_name} has begun.")
    with open(workload_path, "r") as file :
        workload = file.readlines()
       
    db_config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = db_config.user,
        password = db_config.password,
        host = db_config.host,
        port = db_config.port
    ) 
    cursor = conn.cursor()
    cursor.execute(f"set search_path to {schema}")
    conn.commit()
    cursor.close()
    
    ## workload information
    with open(workload_path, 'r') as file :
        sqls = file.readlines()
        logger.info(f"* the num of sqls in this workload is {len(sqls)}")
   
    ## index files information
    filelist = glob.glob(os.path.join(directory_mm, pattern))
    logger.info(f"* The number of index files is {len(filelist)}.")
    logger.info(f"** The paths of index files are as followed >>> {filelist}")
  
    ## initialize the indexes [drop default index of job]
    drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
    drop_index_prefix(conn, drop_index_sql)
     
    ## storage_constraint
    cursor = conn.cursor()
    cursor.execute(f"select pg_database_size('{db_name}');")
    storage_constraint = (cursor.fetchone()[0] / ( 1024 * 1024 )) * index_storage_proportion
    conn.commit()
    cursor.close()
    
    ## test execution
    estimated_costs = []
    logger.info(f"with database {db_name} the first execution for test")
    estimated_cost = query_plan_cost_estimation(workload, conn, schema)
    logger.info(f"** default estimated cost is {estimated_cost}")
    
    ## first execution with default index settings
    logger.info("********************************* Start! *********************************")
    f_estimated_cost = query_plan_cost_estimation(workload, conn, schema)
    logger.info(f"* default estimated cost is {f_estimated_cost}")

    
    ## index creation experiments
    file_names = []
    for idx, path in enumerate(sorted(filelist)):
        logger.info(f"* current index path is {path}")
        drop_index_prefix(conn, drop_index_sql)
        file_names.append(os.path.basename(path))
        
        with open(path, 'r') as file :
            indexes = file.readlines()
        
        ## cost estimation
        hypopg_indexes_creation_constraint(indexes, conn, schema, storage_constraint)
        cost_estimation = query_plan_cost_estimation(workload, conn, schema)
        hypopg_drop_indexes(conn, schema)
        
        estimated_costs.append(cost_estimation)
        ## workload execution
        logger.info(f"* with index file in {path}, what_if_estimation is {cost_estimation}")
        
    # execution_times, f_execution_time           
    radios = []
    radios_ = []
    for ecost in estimated_costs :
        radio = round((f_estimated_cost - ecost) / f_estimated_cost, 4)
        radios.append(f"{radio * 100}% ↓")
        radios_.append(radio * 100)
        

     
    logger.info(f"** workload test has finished.\n")
    
    logger.info(f"** Default execution times --> {f_estimated_cost}.\n")
    logger.info(f"** File names --> {file_names}.\n")
    logger.info(f"** Execution times --> {estimated_costs}.\n")
    logger.info(f"** Radios --> {radios}.\n")
    logger.info(f"** Radios_ --> {radios_}.\n")
    logger.info(f"** workload test has finished.\n")
    logger.info(f"** workload test in database {db_name} has finished.")
    
    with open(output_path, 'a') as file :
        file.write("\n**********************************************************\n")
        for file_name, radio in zip(file_names, radios_) :
            file.write(f"{file_name} : {radio}\n")
