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
from SQLParser import get_ndvs_all, obtain_workload_information, get_db_schema
from functions import execute_sql_view, obtain_default_index_statements, drop_index_prefix

table_dot_column = r'\b[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b'
index_priority = [
    "smallint",     # 小范围整数
    "integer",      # 标准整数
    "bigint",       # 大范围整数
    "serial",       # 自动递增整数
    "bigserial",    # 大范围自动递增整数
    "numeric",      # 精确数值类型
    "real",         # 单精度浮点数
    "double precision",  # 双精度浮点数
    "money",        # 货币类型
    "date",         # 日期类型
    "timestamp",    # 时间戳（无时区）
    "timestamp with time zone",  # 时间戳（有时区）
    "time",         # 时间（无时区）
    "time with time zone",  # 时间（有时区）
    "interval",     # 时间间隔
    "character varying",  # 可变长度字符串 (VARCHAR)
    "character",    # 定长字符串 (CHAR)
    "text",         # 可变长度文本
    "boolean",      # 布尔类型
    "json",         # JSON 数据类型
    "jsonb",        # 二进制 JSON 类型
    "uuid",         # 通用唯一标识符
    "xml",          # XML 数据类型
    "bytea",        # 二进制数据
    "array",        # 数组类型
    "tsvector",     # 全文搜索类型
    "tsquery",      # 全文搜索查询类型
    "cidr",         # IPv4/IPv6 网络地址
    "inet",         # IPv4/IPv6 主机地址
    "macaddr",      # MAC 地址
    "bit",          # 定长位串
    "bit varying"   # 可变长度位串
]

if __name__ == "__main__" :
    ## args
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="")
    parser.add_argument("--bench", type=str, help="benchmark name", default="tpch")
    # parser.add_argument("--demos_lat_path", type=str, help="latency based demos path", default="/home/u2023000897/gpt4index/demos_info/lat/{}.json")
    parser_args = parser.parse_args()
    config_path = parser_args.config
    bench = parser_args.bench
    # demos_lat_path = parser_args.demos_lat_path
    args = json.load(open(config_path, 'r'))
    
    ## definition
    db_name = args["db_name"]
    workload_dir = args["workload_dir"]
    workload_info_dir = args['workload_info_dir']
    demosinfo_output_path = args["demosinfo_output_path"]
    log_path = args["log_path"]
    start_id = args['start_id']
    num = args['num']
    
    if not os.path.exists(os.path.dirname(log_path)) : os.makedirs(os.path.dirname(log_path))
    if not os.path.exists(os.path.dirname(demosinfo_output_path)) : os.makedirs(os.path.dirname(demosinfo_output_path))
    
    db_name_ = db_name
    if db_name == "benchbase" : db_name_ = args["bench_name"]
    data_path = "/home/u2023000897/gpt4index/data"
    schema_path = f"{data_path}/schema/{db_name_}_schema.json"
    ndv_path = f"{data_path}/ndv/{db_name_}_ndv.json"
    
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
    
    drop_index_sql = "select indexname from pg_indexes where schemaname NOT IN ('pg_catalog', 'information_schema') and indexname not like '%_pkey';"
    drop_index_sql = "select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = 'public';"
    drop_index_prefix(conn, drop_index_sql)
    
    demosinfo_dict = {}
    
    if os.path.exists(demosinfo_output_path) :
        with open(demosinfo_output_path, 'r') as file :
            demosinfo_dict = json.load(file)
    demosinfo_dict = {}
    demons_idx = len(list(demosinfo_dict.keys()))
    
    # demos_lat_path_ = demos_lat_path.format(bench)
    # with open(demos_lat_path_, 'r') as json_f :
    #     demosinfo_lat = json.load(json_f)
    # demosinfo_lat_wls = [v['workload'] for k, v in demosinfo_lat.items()]
    demosinfo_lat = {}
    demosinfo_lat_wls = []
        
    for current_num in range(num) :
        current_id = start_id + current_num
        
        demosinfo = {}
        
        ## workload_info
        workload_info_path = workload_info_dir + f"/result_{current_id}.json"
        if not os.path.exists(workload_info_path) : continue
        
        if f"{bench}_workload_{current_num}" in [v['workload'] for v in demosinfo_dict.values()] : 
            continue
        
        logger.info(f"* current workload id --> workload_{current_id}")     
        
        with open(workload_info_path, 'r') as file :
            workload_info = json.load(file)
        demosinfo["workload"] = workload_info['workload']
        workload_path = workload_dir + '/' + workload_info['workload'] + '.sql'
        
        ## with input information of SQLs, selectivity, NDV
        if demosinfo["workload"] in demosinfo_lat_wls :
            cur_k = list(demosinfo_lat.keys())[demosinfo_lat_wls.index(demosinfo["workload"])]
            demosinfo["sorted_used_column_cnts_ndvs"] = demosinfo_lat[cur_k]['sorted_used_column_cnts_ndvs']
            demosinfo["sorted_where_selectivities"] = demosinfo_lat[cur_k]['sorted_where_selectivities']
            demosinfo["sorted_other_predicates"] = demosinfo_lat[cur_k]['sorted_other_predicates']
            demosinfo["sorted_group_order_columns"] = demosinfo_lat[cur_k]['sorted_group_order_columns']
            demosinfo["existing_indexes_keys"] = demosinfo_lat[cur_k]['existing_indexes_keys']
        else :
            ## with input information of SQLs, selectivity, NDVs
            # SQLs
            with open(workload_path, "r") as f:
                workload = f.readlines()
            len_workload = len(workload)
            logger.info("* Get workload")
            
            # schema
            used_column_info = []
            only_column_names = []
            column_types = []
            table_columns = []
            column_rows = []
            
            workload_infos, used_column_info = obtain_workload_information(workload, conn, db_name, schema_path)
            
            for column in used_column_info :
                only_column = column[0].split('.')[1].split(':')[0]
                table_column = column[0].split(':')[0].strip()
                column_type = column[0].split(':')[1].strip()
                only_column_names.append(only_column)
                column_types.append(column_type)
                table_columns.append(table_column)
                column_rows.append(column[1])
            table_column_rows = {k : v for k, v in zip(table_columns, column_rows)}
            
            table_columns = list(table_column_rows.keys())
            logger.info("* Get workload info")
            
            # used_column_counts 
            used_column_counts = {table_column : {'Type' : column_type, 'Counts' : 0} for table_column, column_type in zip(table_columns, column_types)}
            used_column_names = [used_column.lower() for used_column in used_column_counts.keys()]
            where_selectivities = workload_infos['where_selectivities']
            other_predicates = workload_infos['other_predicates']
            group_order_columns = workload_infos['group_order_columns']
            
            # counts
            for join_column, counts in other_predicates.items() :
                if join_column.lower() in list(used_column_names) : used_column_counts[join_column.lower()]['Counts'] += counts
                elif any(join_column.lower().split('.')[-1] in column_n for column_n in list(used_column_counts.keys())) :
                    for column_n in list(used_column_counts.keys()) :
                        if join_column.lower() in column_n : used_column_counts[column_n]['Counts'] += counts
                else :  
                    print(f"Error in extract used columns in join *{join_column.lower()}*")
                    continue
                    exit()

            for go_column, counts in group_order_columns.items() :
                if go_column.lower() in list(used_column_counts.keys()) : used_column_counts[go_column.lower()]['Counts'] += counts
                elif any(go_column.lower().split('.')[-1] in column_n for column_n in list(used_column_counts.keys())) :
                    for column_n in list(used_column_counts.keys()) :
                        if go_column.lower() in column_n : used_column_counts[column_n]['Counts'] += counts
                else : 
                    print(f"Error in extract used columns in group_order *{go_column.lower()}*")
                    continue
                    exit()
            
            where_selectivities_cols = {}
            for where, sele in where_selectivities.items() :
                res = re.findall(table_dot_column, where) 
                if res :
                    for result in res :
                        if result.lower() in list(used_column_counts.keys()) : 
                            used_column_counts[result.lower()]['Counts'] += 1
                            if result.lower() not in where_selectivities_cols.keys() :
                                where_selectivities_cols[result.lower()] = [sele]
                            else :
                                where_selectivities_cols[result.lower()].append(sele)
                        elif any(result.lower().split('.')[-1] in column_n for column_n in list(used_column_counts.keys())) :
                            for column_n in list(used_column_counts.keys()) :
                                if result.lower() in column_n : 
                                    used_column_counts[column_n]['Counts'] += 1
                                    if result.lower() not in where_selectivities_cols.keys() :
                                        where_selectivities_cols[result.lower()] = [sele]
                                    else :
                                        where_selectivities_cols[result.lower()].append(sele)
                        else : 
                            print(f"Error in extract used columns in where *{result.lower()}*")
                            continue
                            exit()
            
            if used_column_counts != {} :
                used_column_counts = {k : v for k, v in used_column_counts.items() if v['Counts'] > 0 }
            logger.info("* Get counts info")
            
            # NDV (number of the distinct value) [workload independent]
            used_column_cnts_ndvs = used_column_counts
            if os.path.exists(ndv_path) :
                with open(ndv_path,'r') as f :
                    ndvs = json.load(f)
                if ndvs == None :
                    ndvs = get_ndvs_all(conn, ndv_path)
            else :
                ndvs = get_ndvs_all(conn, ndv_path) # ratio
                
            # print(used_column_cnts_ndvs)
            for used_column_info in used_column_cnts_ndvs :
                used_column_cnts_ndvs[used_column_info]['NDVs'] = ndvs[used_column_info]
                used_column_cnts_ndvs[used_column_info]['Rows'] = table_column_rows[used_column_info]
                used_column_cnts_ndvs[used_column_info]['NDV_Rows'] = int(ndvs[used_column_info] * table_column_rows[used_column_info])
            logger.info("* Get NDVs")
            
            # sort by the NDVs 
            # sorted_keys = sorted(used_column_cnts_ndvs, key = lambda x : (-used_column_cnts_ndvs[x]['Counts'], -used_column_cnts_ndvs[x]['NDVs'], -used_column_cnts_ndvs[x]['Rows'], index_priority.index(used_column_cnts_ndvs[x]['Type'].lower()) if used_column_cnts_ndvs[x]['Type'] in index_priority else float('inf'), x))
            sorted_keys = sorted(used_column_cnts_ndvs, key = lambda x : (-used_column_cnts_ndvs[x]['NDV_Rows'], -used_column_cnts_ndvs[x]['Counts'], index_priority.index(used_column_cnts_ndvs[x]['Type'].lower()) if used_column_cnts_ndvs[x]['Type'] in index_priority else float('inf'), x))
            # sorted_keys = sorted(used_column_cnts_ndvs, key = lambda x : (-used_column_cnts_ndvs[x]['Counts'], -used_column_cnts_ndvs[x]['NDVs'], index_priority.index(used_column_cnts_ndvs[x]['Type'].lower()) if used_column_cnts_ndvs[x]['Type'] in index_priority else float('inf'), x))
            sorted_used_column_cnts_ndvs = {k : used_column_cnts_ndvs[k] for k in sorted_keys}
            sorted_keys = sorted(where_selectivities, key = lambda x : (where_selectivities[x], x))
            sorted_where_selectivities = {k : where_selectivities[k] for k in sorted_keys}
            sorted_keys = sorted(other_predicates, key = lambda x : (-other_predicates[x], x))
            sorted_other_predicates = {k : other_predicates[k] for k in sorted_keys}
            sorted_keys = sorted(group_order_columns, key = lambda x : (-group_order_columns[x], x))
            sorted_group_order_columns = {k : group_order_columns[k] for k in sorted_keys}
            sorted_keys = sorted(where_selectivities_cols, key = lambda x : (-len(where_selectivities_cols[x]), min(where_selectivities_cols[x])))
            sorted_where_cols_selectivities = {k : where_selectivities_cols[k] for k in sorted_keys}
            
            sql_columns = workload_infos['sql_columns']
            
            # logger.info(f"* sorted_used_column_cnts_ndvs : {sorted_used_column_cnts_ndvs}")
            # logger.info(f"* where_selectivities : {sorted_where_selectivities}")
            # logger.info(f"* other_predicates : {sorted_other_predicates}")
            # logger.info(f"* group_order_columns : {sorted_group_order_columns}")
                
            # existing_indexes
            existing_indexes_keys, index_infos = obtain_default_index_statements(db_name, schema_path) # only consider the default indexes contains pk
            
            ## used_column_cnts_ndvs, sorted_where_selectivities, sorted_other_predicates, sorted_group_order_columns, existing_indexes_keys[primary keys]
            demosinfo["sorted_used_column_cnts_ndvs"] = sorted_used_column_cnts_ndvs
            demosinfo["sorted_where_selectivities"] = sorted_where_selectivities
            demosinfo["sorted_other_predicates"] = sorted_other_predicates
            demosinfo["sorted_group_order_columns"] = sorted_group_order_columns
            demosinfo["where_cols_selectivities"] = where_selectivities_cols
            demosinfo["sql_columns"] = sql_columns
            demosinfo["existing_indexes"] = index_infos
        
        default_cost = workload_info["default_cost"]
        default_used_indexes = workload_info["default_used_indexes"]
        demosinfo["default_cost"] = default_cost
        demosinfo["default_used_indexes"] = default_used_indexes
        
        best_index = True
        for storage_const in workload_info.keys() :
            if "%" in storage_const : 
                demosinfo_sc = {}
                
                if not os.path.exists(workload_info[storage_const]['best_index_path']) :
                    logger.warning(f"* Best index not exists for {workload_info['workload']} in storage {storage_const}, continuing...")
                    best_index = False
                    break
                
                with open(workload_info[storage_const]['best_index_path'], 'r') as file :
                    indexes_ = file.readlines()
                    
                indexes = []
                for index in indexes_ :
                    indexes.append(sqlparse.format(index, keyword_case = 'upper', identifier_case = 'lower'))
                    
                demosinfo_sc["recommended_indexes"] = indexes
                demosinfo_sc["recommended_cost"] = workload_info[storage_const]['best_cost']
                demosinfo_sc["recommended_used_indexes"] = workload_info[storage_const]['best_used_indexes']
                rc = workload_info[storage_const]['best_cost']
                if demosinfo_sc["recommended_cost"] < default_cost :
                    demosinfo_sc["cost_fluctuation"] = f"Reduce {(default_cost - rc)*100/default_cost}%"
                else : 
                    demosinfo_sc["cost_fluctuation"] = f"Increase {(rc - default_cost)*100/default_cost}%"
                drop_index_prefix(conn, drop_index_sql)
                
                demosinfo[storage_const] = demosinfo_sc
        
        if best_index : 
            demosinfo_dict[f"demos_{demons_idx}"] = demosinfo
            logger.debug(f"* demos_{demons_idx} --> {demosinfo}")     
            demons_idx += 1
            with open(demosinfo_output_path, 'w') as file :
                json.dump(demosinfo_dict, file, indent=4) 
    
    exit()