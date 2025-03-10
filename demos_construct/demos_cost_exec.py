import psycopg2
import glob
import os
import argparse
import json
import re
import logging
import statistics
import sys
sys.path.append("..")

from config import DBConfig
from functions import query_plan_cost_estimation_used_indexes


logger = logging.getLogger('log')
logger.setLevel(logging.DEBUG)
    
def parse() :
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=str, help="database name", default="")
    parser.add_argument("--workload", type=str, help="workload path", default="")
    parser.add_argument("--pattern", type=str, help="index file name pattern", default="index*.sql") # default setting is magicmirror format
    parser.add_argument("--directory", type=str, help="index file dir", default="")
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
              
if __name__ == "__main__" :
    ## parse config_bm.json
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="test.json")
    config_path = parser.parse_args().config
    args = json.load(open(config_path, 'r'))
    
    ## parameters
    db_name = args["database"]
    workload_name = args['workload_name']
    pattern = args["pattern"]
    output_path = args["output_path"]
    drop_default_indexes = args["drop_default_indexes"]
    start_id = args['start_id']
    num = args['num']
    num_of_baselines = args['num_of_baselines']
    storage_constraints = args["storage_constraints"]
    result_dir = args["result_dir"]
    multi_workload_dir = args['multi_workload_dir']
    indexes_dir = args['indexes_dir']
    rename = args['rename']

    if not os.path.exists(result_dir) : os.makedirs(result_dir)
    if not os.path.exists(os.path.dirname(output_path)) : os.makedirs(os.path.dirname(output_path))
    
    ## logging
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    with open(output_path, "w") as file :
        file.truncate()
    file_handler = logging.FileHandler(output_path, mode = 'a')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    ## initialize   
    db_config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = db_config.user,
        password = db_config.password,
        host = db_config.host,
        port = db_config.port
    ) 
    
    for current_num in range(num) :
        current_id = start_id + current_num
        logger.info(f"* workload {current_id} in database {db_name} has begun.")
        
        ## workload information
        workload_path = f"{multi_workload_dir}/{workload_name}_{current_id}.sql"
        with open(workload_path, 'r') as file :
            sqls = file.readlines()
            logger.info(f"** the num of sqls in this workload is {len(sqls)}")

        ## start
        logger.info("********************************* Start! *********************************\n")
        
        ## initialize the indexes 
        drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
        if drop_default_indexes : drop_index_prefix(conn, drop_index_sql)
        
        ## first execution with default index settings
        default_cost, default_used_indexes = query_plan_cost_estimation_used_indexes(sqls, conn)
        logger.info(f"* default cost is {default_cost}")
    
        ## result.json [(workload_storageconstraint_indexpath_latency)]
        result_path = f"{result_dir}/result_{current_id}.json"
        result_dict = {}
        result_dict['workload'] = f"{workload_name}_{current_id}"
        result_dict['default_cost'] = default_cost
        result_dict['default_used_indexes'] = default_used_indexes
        
        with open(result_path, 'w') as file :
            json.dump(result_dict, file, indent=4)
        
            
        ## index creation experiments
        storage_constraints_dir = {k : v for k,v in zip(reversed(storage_constraints), reversed(list(range(len(storage_constraints)))))}
        current_best_cost = default_cost
        current_best_index_path = ""
        current_best_used_indexes = default_used_indexes
        costs = []
        used_indexes = []
        for id, storage_idx in enumerate(storage_constraints_dir.items()):
            logger.info(f"** current storage_idx is {storage_idx}")
            storage = storage_idx[0]
            index_file_id = storage_idx[1]
            for i in range(num_of_baselines) :
                if rename : path = f"{indexes_dir}/{workload_name}_{current_id}/rename/index_{index_file_id + i * len(storage_constraints)}.sql"
                else : path = f"{indexes_dir}/{workload_name}_{current_id}/index_{index_file_id + i * len(storage_constraints)}.sql"
                logger.info(f"* current index path is {path}")
                drop_index_prefix(conn, drop_index_sql)
                is_idx = execute_sql_only(conn, path)  # index creation
                if is_idx :
                    logger.info("** Indexes created")
                    current_cost, cur_used_indexes = query_plan_cost_estimation_used_indexes(sqls, conn)
                    logger.info(f"* with index file in {path}, current_cost is {current_cost}")
                    if current_cost < current_best_cost :
                        current_best_cost = current_cost
                        current_best_index_path = path
                        current_best_used_indexes = cur_used_indexes
                    costs.append(current_cost)
                    used_indexes.append(cur_used_indexes)
                else :
                    costs.append(default_cost)
                    used_indexes.append(default_used_indexes)
            # write results
            res = {}
            res['costs'] = costs
            res['used_indexes'] = used_indexes
            res['best_index_path'] = current_best_index_path
            res['best_cost'] = current_best_cost
            res['best_used_indexes'] = current_best_used_indexes
            result_dict[storage] = res 
            
            current_best_cost = default_cost
            current_best_index_path = ""
            current_best_used_indexes = default_used_indexes
            costs = []
            used_indexes = []
                    
            with open(result_path, 'w') as file :
                json.dump(result_dict, file, indent=4)
             
            
        logger.info(f"** Finished! **")
    