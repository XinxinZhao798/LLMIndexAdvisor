## same form as [demos_1.json] : demo_name --> existing_indexes_key, used_indexes, latency, cost, reduction * 2

# whole process similar to demos_append.py


## demonstrations' informations
## --> input information : used_column_infos, where_selectivities, join_columns, group_order_columns, existing_indexes, storage_constraints
## --> output information : indexes, lat_variation, cost_variation [cost before and after indexes]
import psycopg2
import glob
import os
import argparse
import json
import re
import logging
import sys
import sqlparse
sys.path.append("..")

from config import DBConfig
from SQLParser import get_db_schema
from functions import drop_index_prefix, query_plan_cost_estimation_used_indexes, execute_sql_index_creation, obtain_default_index_statements, extract_index_info

storage2path = {'10%': 4, '20%' : 3, '30%' : 2, '40%' : 1, '50%' : 0}
storage2path = {'10%': 5, '20%' : 4, '30%' : 3, '40%' : 2, '50%' : 1, '60%' : 0}

def generated_recom_index_statements(existing_indexes, best_indexes) :
    recom_index_statements = []
    pattern = r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(?:USING\s+\w+\s+)?\(([^)]+)\)"
    
    ex_indexes_names = []
    best_indexes_names = []
    for existing_index in existing_indexes :
        match = re.search(pattern, existing_index, re.IGNORECASE)
        if match :
            ex_indexes_names.append(match.group(1))
    for best_index in best_indexes :
        match = re.search(pattern, best_index, re.IGNORECASE)
        if match :
            best_indexes_names.append(match.group(1))
    
    for ex_indexes_name in ex_indexes_names :
        if ex_indexes_name not in best_indexes_names and '_pkey' not in ex_indexes_name :
            recom_index_statements.append(sqlparse.format(f'drop index {ex_indexes_name};\n', keyword_case = 'upper', identifier_case = 'lower'))
    
    for best_indexes_name in best_indexes_names :
        if best_indexes_name not in ex_indexes_names :
            for best_index in best_indexes :
                if best_indexes_name in best_index.split() :
                    recom_index_statements.append(sqlparse.format(best_index, keyword_case = 'upper', identifier_case = 'lower'))  
                    break
          
    return recom_index_statements 
          
if __name__ == "__main__" :
    ## args
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="")
    config_path = parser.parse_args().config
    args = json.load(open(config_path, 'r'))
    
    ## definition
    db_name = args["db_name"]
    workload_dir = args["workload_dir"]
    workload_info_dir = args['workload_info_dir']
    demosinfo_output_path = args["demosinfo_output_path"]
    log_path = args["log_path"]
    start_id = args['start_id']
    num = args['num']
    indexes_dir = args['indexes_dir']
    rename = args['rename']
    
    data_path = "/home/u2023000897/gpt4index/data"
    schema_path = f"{data_path}/schema/{db_name}_schema.json"
    ndv_path = f"{data_path}/ndv/{db_name}_ndv.json"
    
    ## logging
    logger = logging.getLogger('log')
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    if args["logger_console"] == "INFO" : console_handler.setLevel(logging.INFO)
    else : console_handler.setLevel(logging.DEBUG)

    with open(log_path, "w") as file :
        file.truncate()
    file_handler = logging.FileHandler(log_path, mode = 'a')
    if args["logger_file"] == "INFO" : file_handler.setLevel(logging.INFO)
    else : file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    ## connect to remote database
    config = DBConfig()
    conn = psycopg2.connect(
        dbname = db_name,
        user = config.user,
        password = config.password,
        host = config.host,
        port = config.port
    ) 
    get_db_schema(db_name, schema_path)   
    
    drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
    drop_index_prefix(conn, drop_index_sql)
    
    demosinfo_dict = {}
    demons_idx = 0
    
    if os.path.exists(demosinfo_output_path) :
        with open(demosinfo_output_path, 'r') as file :
            demosinfo_dict = json.load(file)
    demosinfo_dict = {}
    
    existing_indexes_keys, index_infos = obtain_default_index_statements(db_name, schema_path)
    
    for current_num in range(num) :
        current_id = start_id + current_num
        
        demosinfo = {}
        
        ## workload_info
        workload_info_path = workload_info_dir + f"/result_{current_id}.json"
        if not os.path.exists(workload_info_path) : continue
        
        if f"demos_{demons_idx}" in demosinfo_dict.keys() and f"demos_{demons_idx}" != list(demosinfo_dict.keys())[-1]: continue
        
        logger.info(f"* current workload id --> workload_{current_id}")     
        
        with open(workload_info_path, 'r') as file :
            workload_info = json.load(file)
        demosinfo["workload"] = workload_info['workload']
        
        workload_path = workload_dir + '/' + workload_info['workload'] + '.sql'
        with open(workload_path, "r") as f:
            workload = f.readlines()
        
        drop_index_prefix(conn, drop_index_sql)
        default_cost = workload_info['default_cost']
        default_used_indexes = workload_info['default_used_indexes']
        default_used_indexes_len = len(default_used_indexes)
        
        best_index = True
        for storage_const in workload_info.keys() :
            if "%" in storage_const : 
                demos_indexes_res = workload_info[storage_const]['costs']
                demos_indexes_used = workload_info[storage_const]['used_indexes']
                demosinfo_scs = []
                demosinfo_sc = {}
                
                if not os.path.exists(workload_info[storage_const]['best_index_path']) :
                    logger.warning(f"* Best index not exists for {workload_info['workload']} in storage {storage_const}, continuing...")
                    best_index = False
                    break
                
                ## get best results [indexes, indexes_len][best_indexes, best_used_indexes, best_used_indexes_len]
                with open(workload_info[storage_const]['best_index_path'], 'r') as file :
                    best_indexes = file.readlines()
                best_indexes_len = len(best_indexes)
                best_used_indexes = workload_info[storage_const]['best_used_indexes']
                best_used_indexes_len = len(best_used_indexes)
                best_cost = workload_info[storage_const]['best_cost']
                
                drop_index_prefix(conn, drop_index_sql)
                
                other_results = []
                # generate refine demos for evert suboptimal demos
                for i in range(len(demos_indexes_res)) :
                    index_file_id = storage2path[storage_const] + 5 * i
                    if rename : index_file_path = indexes_dir + workload_info['workload'] + f'/rename/index_{index_file_id}.sql'
                    else : index_file_path = indexes_dir + workload_info['workload'] + f'/index_{index_file_id}.sql'
                    # index_file_path
                    with open(index_file_path, 'r') as file :
                        indexes_ = file.readlines()
                        
                    indexes = []
                    for index in indexes_ :
                        indexes.append(sqlparse.format(index, keyword_case = 'upper', identifier_case = 'lower'))
                        
                    cost_i = demos_indexes_res[i]
                    used_indexes_i = workload_info[storage_const]['used_indexes'][i]
                    other_results.append({'index_file_path' : index_file_path, 'indexes' : indexes, 'cost' : cost_i, 'used_indexes' : used_indexes_i})
                    if indexes != best_indexes and cost_i != best_cost : # gen refine demos
                        existing_indexes_keys_i = list(set(existing_indexes_keys).union(set(indexes)))
                        existing_indexes_infos = extract_index_info(existing_indexes_keys_i)
                        demosinfo_sc['existing_indexes'] = existing_indexes_infos
                        demosinfo_sc['default_cost'] = cost_i
                        demosinfo_sc['default_used_indexes'] = used_indexes_i
                        recom_indexes_statements = generated_recom_index_statements(existing_indexes_keys_i, best_indexes)
                        demosinfo_sc['recom_indexes_statements'] = recom_indexes_statements
                        demosinfo_sc['recommended_used_indexes'] = best_used_indexes
                        demosinfo_scs.append(demosinfo_sc)
                
                demosinfo[storage_const] = {'demos' : demosinfo_scs, 'other_results' : other_results}
        
        if best_index : 
            demosinfo_dict[f"demos_{demons_idx}"] = demosinfo
            
            # logger.debug(f"* demos_{demons_idx} --> {demosinfo}")     
            demons_idx += 1
            
            with open(demosinfo_output_path, 'w') as file :
                json.dump(demosinfo_dict, file, indent=4)  
            
            # exit()
    exit()