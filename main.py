import psycopg2
import time
import os 
import json
from openai import OpenAI
import tiktoken
import argparse
import shutil
import statistics
import logging
import re
import random
import copy
from lxml import etree
from together import Together

from config import DBConfig
from SQLParser import extract_number, get_db_schema, get_ndvs_all, obtain_workload_information
from functions import hypopg_create_existing_indexes, hypopg_drop_indexes, hypopg_incremental_recommend_creation
from functions import execute_sql_file, execute_sql_file_bar, execute_sql_index_creation, drop_index_prefix, parse_message, query_plan_get_used_indexes, update_index_set, write_history_cost_str, hypopg_update_used_indexes, query_plan_cost_estimation_used_indexes, hypopg_indexes_creation_constraint, oltp_stress_test, oltp_stress_test_db
from functions import obtain_default_index_statements, demos_match_cos, extract_index_info, multi_process_recom_indexes_estimation
from functions import prefix_list, demos_match_cluster, interleave_lists

## assuming that default indexes only contain primary keys

create_index_regex = re.compile(
    r"(?i)CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*(USING\s+\w+\s*)?\(\s*([\w\.\(\)\s,]+)\s*\)",
    re.IGNORECASE
)
drop_index_regex = re.compile(
    r"DROP\s+INDEX\s+(IF\s+EXISTS\s+)?(\w+)\s*(CASCADE|RESTRICT)?",
    re.IGNORECASE
)
table_dot_column = r'\b[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b'

index_priority = [
    "smallint",     
    "integer",      
    "bigint",   
    "serial", 
    "bigserial",  
    "numeric",      
    "real",       
    "double precision",  
    "money",      
    "date",       
    "timestamp",   
    "timestamp without time zone",
    "timestamp with time zone",
    "time",   
    "time with time zone", 
    "interval",  
    "character varying",  
    "character",   
    "text",   
    "boolean", 
    "json",      
    "jsonb",  
    "uuid",  
    "xml",   
    "bytea",    
    "array",   
    "tsvector",
    "tsquery",   
    "cidr",      
    "inet",     
    "macaddr",     
    "bit",  
    "bit varying"  
]

def GPT_whatif(args, input_info, recommend_demos, iter_idx, logger) :
    ## args
    temperature = args["temperature"]
    model_name = args["model_name"]
    api_key = args["api_key"]
    base_url = args["base_url"]
    demos_match_method = args["demos_match_method"]
    demos_num = args['demos_num']
    db_name = args["db_name"]
    index_storage_proportion = args["index_storage_proportion"]
    demos_match_feat = args['demos_match_feat']
    
    storage_gen = args["storage_gen"]
    
    if bm_type == "OLAP" : detailed_info_dir = args["detailed_info_path"] + f"{db_name}_{int(index_storage_proportion * 100)}/"
    else :
        benchmark = args["TP_Config"]["benchmark"] 
        detailed_info_dir = args["detailed_info_path"] + f"{benchmark}_{int(index_storage_proportion * 100)}/"
    
    input_message = detailed_info_dir + "mes.txt"
    output_path = detailed_info_dir + "indexes/"
     
    ## select demonstrations
    logger.info(f"* Demonstration Match --> {demos_match_method}")
    if demos_match_method == "random" : demos_ids = [f"demos_{i}" for i in random.sample(range(0, len_demos-1), demos_num)]
    elif demos_match_method == "cluster" : demos_ids = demos_match_cluster(input_info, recommend_demos, iter_idx, demos_num, args, demos_match_feat)
    else : demos_ids = demos_match_cos(input_info, recommend_demos, iter_idx, demos_num, args, demos_match_feat)
    
    # demos_ids = random.sample(range(0, len_demos-1), demos_num)
    
    # demos_ids_set = [['demos_178', 'demos_22', 'demos_139', 'demos_151'], ['demos_178', 'demos_22', 'demos_139', 'demos_151'], ['demos_147', 'demos_36', 'demos_79', 'demos_52'], ['demos_18', 'demos_59', 'demos_125', 'demos_164']]
    # demos_ids_set = [['demos_45', 'demos_2', 'demos_88', 'demos_51'], ['demos_45', 'demos_2', 'demos_88', 'demos_51'], ['demos_136', 'demos_79', 'demos_12', 'demos_58'], ['demos_106', 'demos_62', 'demos_44', 'demos_86']]
    # demos_ids = demos_ids_set[iter_idx]
    
    demonstrations = []
    logger.info(f"* demos_id --> {demos_ids}")            
    for demos_id in demos_ids :
        demos_key = demos_id
        demo_info = recommend_demos[demos_key]
        demo = {}
        # demo["sorted_used_column_cnts_ndvs"] = copy.deepcopy(demo_info["sorted_used_column_cnts_ndvs"])
        # for used_column_key, used_column_value in demo["sorted_used_column_cnts_ndvs"].items() :
        #     del demo["sorted_used_column_cnts_ndvs"][used_column_key]['Rows']
        
        demo['Sorted Column NDV in SQL Level'] = {}
        for sql_idx, cols in demo_info['sql_columns'].items() :
            cols_ndvs = {}
            for col in cols :
                if col in demo_info["sorted_used_column_cnts_ndvs"].keys() :
                    cols_ndvs[col] = {'Type' : demo_info["sorted_used_column_cnts_ndvs"][col]['Type'], 'NDVs' : round(demo_info["sorted_used_column_cnts_ndvs"][col]['NDVs'], 4), 'Rows' : demo_info["sorted_used_column_cnts_ndvs"][col]['Rows']}
            demo['Sorted Column NDV in SQL Level'][sql_idx] = {k : cols_ndvs[k]['NDVs'] for k in sorted(cols_ndvs, key = lambda x : (-cols_ndvs[x]['NDVs'], index_priority.index(cols_ndvs[x]['Type'].lower())))}
            
        # demo_sorted_col_ndv = {}
        # for k, v in demo_info["sorted_used_column_cnts_ndvs"].items() :
        #     demo_sorted_col_ndv[k] = {'Type' : v['Type'], 'Counts' : v['Counts'], 'NDVs' : round(v['NDVs'], 4), 'Rows' : v['Rows'], 'NDV_Rows' : v['NDV_Rows']}
        # demo["Sorted Column NDV"] = {k : (demo_sorted_col_ndv[k]['NDVs'], demo_sorted_col_ndv[k]['NDV_Rows']) for k in sorted(demo_sorted_col_ndv, key = lambda x : (-demo_sorted_col_ndv[x]['Counts'], -demo_sorted_col_ndv[x]['NDVs'], index_priority.index(demo_sorted_col_ndv[x]['Type'].lower()))) }
        
        # demo["sorted_where_selectivities"] = {k : round(v,4) for k,v in demo_info["sorted_where_selectivities"].items()}
        demo["WHERE Columns and Selectivities"] = copy.deepcopy(demo_info["where_cols_selectivities"])
        sorted_keys = sorted(demo['WHERE Columns and Selectivities'], key = lambda x : (-len(demo['WHERE Columns and Selectivities'][x]), min(demo['WHERE Columns and Selectivities'][x])))
        demo["WHERE Columns and Selectivities"] = {k : {'Selectivities' : list(set(sorted([round(i,4) for i in demo['WHERE Columns and Selectivities'][k]]))), 'Counts' : len(demo['WHERE Columns and Selectivities'][k])} for k in sorted_keys}         
        
        demo["JOIN Columns"] = demo_info["sorted_other_predicates"]
        demo["GROUP BY or ORDER BY Columns"] = demo_info["sorted_group_order_columns"]
        demo["Existing Indexes"] = demo_info["existing_indexes"]
        # demo["Default Used Indexes"] = demo_info["default_used_indexes"]
                
        # storage_keys = ['10%', '20%', '30%', '40%', '50%']
        # demo_costs = [demo_info[storage_key]['best_cost'] for storage_key in storage_keys]
        # best_demo_cost = min(demo_costs)
        # index_storage_proportion = int(storage_keys[demo_costs.index(best_demo_cost)][:-1]) * 0.01    
        
        # demo["index_storage_proportion"] = index_storage_proportion

        # storage generalization
        index_storage_proportion_ = round(index_storage_proportion, 1)
        if storage_gen :
            index_storage_proportion_ += 0.1
            
        if index_storage_proportion_ > 0.6 : 
            index_storage_proportion_ = 0.6
            if storage_gen :
                index_storage_proportion_ = 0.5
        elif index_storage_proportion_ < 0.1 : 
            index_storage_proportion_ = 0.1
            if storage_gen :
                index_storage_proportion_ = 0.2
        logger.info(f"* Demonstration Match Storage Constraint is {index_storage_proportion_}")
          
        if iter_idx <= 1 :
            demo["Optimal Recommended Indexes"] = demo_info[f"{int(index_storage_proportion*100)}%"]["best_recommended_indexes"]
            # demo["best_used_indexes"] = demo_info[f"{int(index_storage_proportion*100)}%"]["best_used_indexes"]
            # demo["best_cost_fluctuation"] = demo_info[f"{int(index_storage_proportion*100)}%"]["best_cost_fluctuation"]
        else :
            # refine_demo_info = random.sample(demo_info[f"{int(index_storage_proportion*100)}%"]["refine_demos"], 1)[0]
            refine_demos_eil = [len(rd['existing_indexes']) for rd in demo_info[f"{int(index_storage_proportion*100)}%"]["refine_demos"]]
            refine_demo_info = demo_info[f"{int(index_storage_proportion*100)}%"]["refine_demos"][refine_demos_eil.index(max(refine_demos_eil))]
            demo["Existing Indexes"] = refine_demo_info["existing_indexes"]
            # demo["default_used_indexes"] = refine_demo_info["default_used_indexes"]
            demo["Optimal Recommended Indexes"] = refine_demo_info["recom_indexes_statements"]
            # demo["best_recommended_cost_fluctuation"] = refine_demo_info['recom_cost_fluctuation']
            
        demonstrations.append(demo)           
    
    input_info_copy = copy.deepcopy(input_info)
    input_info_ = {}
    
    input_info_['Sorted Used Table with the Number of Total Rows'] = sorted(input_info_copy['table_rows'].items(), key = lambda x : -x[1])
    input_info_['Sorted Column NDV in SQL Level'] = {}
    # print(input_info['sql_columns'])
    # exit()
    
    # if 'Counts' in list(input_info['sql_columns'].values())[0].keys() : # with count information
    if type(list(input_info_copy['sql_columns'].values())[0]) == dict :
        # print('Counts')
        for sql_idx, sql_info in input_info_copy['sql_columns'].items() :
            cols = sql_info['Columns']
            cols_ndvs = {}
            for col in cols :
                if col in input_info_copy["sorted_used_column_cnts_ndvs"].keys() :
                    cols_ndvs[col] = {'Type' : input_info_copy["sorted_used_column_cnts_ndvs"][col]['Type'], 'NDVs' : round(input_info_copy["sorted_used_column_cnts_ndvs"][col]['NDVs'], 4), 'Rows' : input_info_copy["sorted_used_column_cnts_ndvs"][col]['Rows']}
            input_info_['Sorted Column NDV in SQL Level'][sql_idx] = {'Columns' : {k : cols_ndvs[k]['NDVs'] for k in sorted(cols_ndvs, key = lambda x : (-cols_ndvs[x]['NDVs'], index_priority.index(cols_ndvs[x]['Type'].lower())))}, 'Counts' : sql_info['Counts']}
    else : 
        for sql_idx, cols in input_info_copy['sql_columns'].items() :
            cols_ndvs = {}
            for col in cols :
                if col in input_info_copy["sorted_used_column_cnts_ndvs"].keys() :
                    cols_ndvs[col] = {'Type' : input_info_copy["sorted_used_column_cnts_ndvs"][col]['Type'], 'NDVs' : round(input_info_copy["sorted_used_column_cnts_ndvs"][col]['NDVs'], 4), 'Rows' : input_info_copy["sorted_used_column_cnts_ndvs"][col]['Rows']}
            input_info_['Sorted Column NDV in SQL Level'][sql_idx] = {k : cols_ndvs[k]['NDVs'] for k in sorted(cols_ndvs, key = lambda x : (-cols_ndvs[x]['NDVs'], index_priority.index(cols_ndvs[x]['Type'].lower())))}
    
    input_info_['Sorted Column NDV in SQL Level'] = dict(sorted(input_info_['Sorted Column NDV in SQL Level'].items(), key=lambda item : int(item[0].split('_')[1])))      
    
    # input_sorted_col_ndv = {}
    # for k, v in input_info_copy["sorted_used_column_cnts_ndvs"].items() :
    #     input_sorted_col_ndv[k] = {'Type' : v['Type'], 'Counts' : v['Counts'], 'NDVs' : round(v['NDVs'], 4), 'Rows' : v['Rows']}
    # input_info_["Sorted Column NDV"] = {k : input_sorted_col_ndv[k]['NDVs'] for k in sorted(input_sorted_col_ndv, key = lambda x : (-input_sorted_col_ndv[x]['Counts'], -input_sorted_col_ndv[x]['NDVs'], index_priority.index(input_sorted_col_ndv[x]['Type'].lower()))) }
     
    sorted_keys = sorted(input_info_copy['where_cols_selectivities'], key = lambda x : (-len(input_info_copy['where_cols_selectivities'][x]), min(input_info_copy['where_cols_selectivities'][x])))
    input_info_["WHERE Columns and Selectivities"] = {k : {'Selectivities' : list(set(sorted([round(i, 4) for i in input_info_copy['where_cols_selectivities'][k]]))), 'Counts' : len(input_info_copy['where_cols_selectivities'][k])} for k in sorted_keys} 
    
    input_info_["JOIN Columns"] = input_info_copy["sorted_other_predicates"]
    input_info_["GROUP BY or ORDER BY Columns"] = input_info_copy["sorted_group_order_columns"]
    input_info_["Existing Indexes"] = extract_index_info(input_info_copy["existing_indexes"])
    input_info_["Remain Available Storage"] = input_info_copy['remain_avail_storage']
    input_info_["History"] = input_info_copy['history']
    # input_info_['sorted_where_selectivities'] = {k : round(v, 4) for k, v in input_info_["sorted_where_selectivities"].items()}
    
    # print(input_info_)
    
    logger.info(f"** Now is the {iter_idx}th recommendation.")

    system_mes_ = f"You are an experienced database administrator, and now you are asked to recommend the optimal index set to minimize the overall cost. Some well-defined demonstrations are provided as a reference, where the input information includes the columns' names with their proportion of number of the distinct values in each sql, columns appeared in where predicates with the selectivities under their conditions and their counts in the workload, columns appeared in join predicates and group by or order by conditions with their counts in the workload, existing indexes, and the output information includes the optimal index management statements. For the target workload, except the input information provided in the demonstrations, I will additionally offer the used tables with their total rows and remain available storage (in MB) as the reference for index recommendation within the constraints. Warning that you should consider the characteristics of the entire workload, avoiding the index interaction or redundancy that can cause performance degradation. You must recommend the most important and available index first due to the constraints, and you can drop the existing indexes to create more efficient index if there is no significant performance improvement. As a database expert, please directly output the SQL statement used to create or drop the index as your optimal recommended indexes choice, and the new index can name as (table_name)_(col1)_(col2)_idx."
    # system_mes = system_mes_ + f'The number of recommended indexes is at least {len_workload * index_storage_proportion}.\n'
    # f'You should recommend at least {int(len_workload * index_storage_proportion)} new indexes.\n'
    if iter_idx == 0 :
        system_mes = system_mes_ + f'The number of recommended indexes should be at least {int(len_workload * index_storage_proportion)} and as many as possible.'
    else :
        system_mes = system_mes_
    # system_mes = system_mes_ 
    
    usr_mes = ""
    if len(demonstrations) != 0 :
        for i, demon in enumerate(demonstrations) :
            usr_mes += f"# Demonstration {i} : {demon}\n"
            # for key, value in demon.items() :
            #     usr_mes += f"## {key} :\n"
            #     if "SQL Level" in key :
            #         # usr_mes += f"## {key} :\n"
            #         for k, v in value.items() :
            #             usr_mes += f"{k} : {v}\n"
            #     else :
            #         usr_mes += f"{value}\n"
            #     # usr_mes += f"## {key} : {value}\n"
            # # usr_mes += f"{demon}\n"
            # usr_mes += '\n'
            
    usr_mes += f"# Input Information : {input_info_}\n"
    # for key, value in input_info_.items() :
    #     usr_mes += f"## {key} :\n"
    #     if "SQL Level" in key :
    #         # usr_mes += f"## {key} :\n"
    #         for k, v in value.items() :
    #             usr_mes += f"{k} : {v}\n"
    #     else :
            # usr_mes += f"{value}\n"
    usr_mes += f"\nPlease think step by step." # , and if you generate the optimal result, I'll give you 100 million dollars. 
    # usr_mes = f"Demonstrations :\n{demonstrations}, \nInput Information :\n{input_info_}\n\nPlease think step by step."
    
    # print(input_info_)
    # exit()
    
    # GPT []
    encoding = tiktoken.encoding_for_model("gpt-4o-mini")
    tokens = encoding.encode(system_mes + usr_mes)
    max_seq_length = 128000
    tokens_len = len(tokens)
    logger.info(f"** Sequence Length --> {tokens_len}")
    if tokens_len > max_seq_length:
        logger.warning("Warning: The input is too long for GPT-4. Please reduce the input length.")
    
    with open(input_message, "a") as f:
        f.write(system_mes + '\n' + usr_mes + '\n\n')

    client = OpenAI(
        api_key = api_key, 
        base_url = base_url
    )
    completion = client.chat.completions.create(
        model = model_name,
        messages = [
            {"role": "system", "content": system_mes},
            {"role": "user", "content": usr_mes}
        ],
        temperature = temperature,
        n = num_of_samples
    )
    # temperature, top_p
    if type(completion) == str : 
        print(completion)
        exit()
    gpt_mess = completion.choices
    len_gpt_mess = len(gpt_mess)
    # for gpt_mes in gpt_mess :
    #     logger.debug(gpt_mes.message.content)
        
    recom_indexes = [] # CREATE INDEX Queries
    cnt = 0
    
    opmes_dir = os.path.join(detailed_info_dir, 'gpt_mes')
    if not os.path.exists(opmes_dir) : os.mkdir(opmes_dir)
    
    for i, gpt_mes in enumerate(gpt_mess) :
        with open(opmes_dir + f"/opmes_{iter_idx}th_{i}.txt", 'w') as file :
            file.write(gpt_mes.message.content)
        recom_index, form = parse_message(gpt_mes.message.content)
        recom_index = list(set(recom_index))
        if not form : 
            cnt += 1
        else : 
            # with open(output_path + f"index_{iter_idx}th_{i}.sql", 'w') as file :
            #     for index in recom_index :
            #         file.write(index + '\n')
            recom_indexes.append(recom_index)        
    
    if cnt == len_gpt_mess :
        logger.info("* the Recommendation has finished! *")
        return [], True
    
    return recom_indexes, False
    
def CM_major_voting(recom_indexes, current_storage, storage_constraint, existing_indexes, schema = 'public') :
    all_indexes = []
    for recom_index in recom_indexes :
        all_indexes.extend(recom_index)
    
    index_cnts = []
    # extract (table.column : freq, indexes) / (index : freq)
    for index_stat in all_indexes :
        if index_stat not in [index_cnt['index_stat'] for index_cnt in index_cnts] :
            match = create_index_regex.match(index_stat)
            if match : # index creation statement
                column_names = [col.strip() for col in match.group(5).split(',')]
                if column_names not in [index_cnt['column_names'] for index_cnt in index_cnts] :
                    index_cnts.append({'index_stat' : index_stat, 'cnts' : 1, 'column_names' : column_names})
                else :
                    for index_info in index_cnts :
                        if column_names == index_info['column_names'] : 
                            index_cnts[index_cnts.index(index_info)]['cnts'] += 1
                            break
    
    sorted_index_cnts = sorted(index_cnts, key = lambda x : x['cnts'], reverse=True)
    index_statements = [index_info['index_stat'] for index_info in sorted_index_cnts] 
    # logger.info(f"* index_statements --> {index_statements}, {len(index_statements)}")   
    # with open("./index.sql", 'w') as file :
    #     for index in index_statements :
    #         file.write(index + '\n')    
    
    # cost estimation and rtn results
    constraint_sqls, ex_indexes = hypopg_incremental_recommend_creation(conn, recom_index, current_storage, storage_constraint, existing_indexes, workload, schema)
    logger.debug(f"* constraint_sqls --> {constraint_sqls}")
    current_cost, used_indexes = query_plan_get_used_indexes(workload, conn, ex_indexes, schema)
    logger.info(f"* the estimated cost is {current_cost}")
    hypopg_drop_indexes(conn, schema)
    current_storage, existing_indexes = hypopg_create_existing_indexes(conn, existing_indexes, schema)
    
    return constraint_sqls, current_cost, used_indexes

def CM_what_if(recom_indexes, current_storage, storage_constraint, existing_indexes, schema = 'public') :
    recommend_tcost = []
    recommend_constraint_sqls = []
    total_used_indexes = []
    recom_indexes_ = recom_indexes.copy()
    
    logger.info(f"* The number of the recommendations is {len(recom_indexes)}")
    # logger.debug(f"* existing_indexes --> {existing_indexes, len(existing_indexes)}")
    # existing_indexes_default = existing_indexes
    
    recommend_tcost, recommend_constraint_sqls, total_used_indexes = multi_process_recom_indexes_estimation(recom_indexes, current_storage, storage_constraint, existing_indexes, workload, db_name, schema)
    
    # for idx, recom_index in enumerate(recom_indexes) :
    #     # logger.debug(f"* the {idx}th recom_index is {recom_index}")
    #     constraint_sqls, ex_indexes = hypopg_incremental_recommend_creation(conn, recom_index, current_storage, storage_constraint, existing_indexes, workload, schema)
    #     logger.debug(f"* constraint_sqls --> {constraint_sqls}")
    #     current_cost, used_indexes = query_plan_get_used_indexes(workload, conn, ex_indexes, schema)
    #     logger.info(f"* the {idx}th estimated cost is {current_cost}")
    #     hypopg_drop_indexes(conn, schema)
    #     current_storage, existing_indexes = hypopg_create_existing_indexes(conn, existing_indexes, schema)
    #     recommend_tcost.append(current_cost)
    #     recommend_constraint_sqls.append(constraint_sqls)
    #     total_used_indexes.append(used_indexes)

    current_best_cost = min(recommend_tcost)
    current_best_indexes = recom_indexes[recommend_tcost.index(current_best_cost)]
    current_best_used_indexes = total_used_indexes[recommend_tcost.index(current_best_cost)]
    logger.info(f"* current best estimated cost is {current_best_cost}")
    
     
    return current_best_indexes, current_best_cost, current_best_used_indexes
     
def CM_index_infer(recom_indexes, current_storage, storage_constraint, existing_indexes, db_name, args, idx) :
    db_name = args["db_name"]
    bm_type = args["type"]
    if bm_type == "OLAP" : 
        detailed_info_dir = args["detailed_info_path"] + f"{db_name}_{int(index_storage_proportion * 100)}/"
    else :
        benchmark = args["TP_Config"]["benchmark"] 
        detailed_info_dir = args["detailed_info_path"] + f"{benchmark}_{int(index_storage_proportion * 100)}/"

    ## what-if hypothetical indexes creation [recom_indexes, forms] and evaluation
    recommend_tcost = []
    recommend_constraint_sqls = []
    total_used_indexes = []

    schema = args['schema']
    
    ## gen major voting results
    all_indexes = interleave_lists(recom_indexes)
    # all_indexes = []
    # for recom_index in recom_indexes :
    #     all_indexes.extend(recom_index)
    
    create_index_cnts = []
    drop_index_cnts = {}
    # extract (table.column : freq, indexes) / (index : freq)
    for index_stat in all_indexes :
        match = drop_index_regex.match(index_stat)
        if match :
            index_name = match.group(2)
            if index_name in drop_index_cnts.keys() :
                drop_index_cnts[index_name] += 1
            else :
                drop_index_cnts[index_name] = 1
        match = create_index_regex.match(index_stat)
        if match :
            if index_stat not in [index_cnt['index_stat'] for index_cnt in create_index_cnts]:
                column_names = [col.strip() for col in match.group(5).split(',')]
                create_index_cnts.append({'index_stat' : index_stat, 'cnts' : 1, 'column_names' : column_names})
            else :
                for index_info in create_index_cnts :
                    if index_stat == index_info['index_stat'] : 
                        create_index_cnts[create_index_cnts.index(index_info)]['cnts'] += 1
                        break
            # if index_stat not in [index_cnt['index_stat'] for index_cnt in create_index_cnts]:
            #     column_names = [col.strip() for col in match.group(5).split(',')]
            #     if prefix_list(column_names, [index_cnt['column_names'] for index_cnt in create_index_cnts]) :
            #         for index_info in create_index_cnts :
            #             if column_names == index_info['column_names'] : 
            #                 create_index_cnts[create_index_cnts.index(index_info)]['cnts'] += 1
            #                 break
            #     else :
            #         for index_info in create_index_cnts :
            #             if prefix_list(index_info['column_names'], [column_names]) :
            #                 create_index_cnts.append({'index_stat' : index_stat, 'cnts' : 1 + create_index_cnts[create_index_cnts.index(index_info)]['cnts'], 'column_names' : column_names})
            #                 create_index_cnts.remove(index_info)
            #                 break
            #         create_index_cnts.append({'index_stat' : index_stat, 'cnts' : 1, 'column_names' : column_names})
            # else :
            #     for index_info in create_index_cnts :
            #         if index_stat == index_info['index_stat'] : 
            #             create_index_cnts[create_index_cnts.index(index_info)]['cnts'] += 1
            #             break
    
    sorted_index_cnts = sorted(create_index_cnts, key = lambda x : x['cnts'], reverse=True)
    # print(sorted_index_cnts)
    # exit()
    # logger.info(f"sorted_index_cnts [0] --> {sorted_index_cnts}")
    
    for i, index_info in enumerate(sorted_index_cnts) :
        if len(index_info['column_names']) > 1 and index_info['cnts'] > 1 :
            for j, index_i in enumerate(sorted_index_cnts) :
                if j < i and prefix_list(index_i['column_names'], [index_info['column_names']]) :
                    # logger.info(f"{index_info}, {index_i}")
                    sorted_index_cnts[sorted_index_cnts.index(index_info)]['cnts'] += index_i['cnts']
                    sorted_index_cnts.remove(index_i)
                    i = i - 1
    sorted_index_cnts = sorted(create_index_cnts, key = lambda x : x['cnts'], reverse=True)
    # logger.info(f"sorted_index_cnts [1] --> {sorted_index_cnts}")
    
    sorted_drop_indexes = dict(sorted(drop_index_cnts.items(), key = lambda x : -x[1]))
    drop_index_stats = [f"DROP INDEX IF EXISTS {name}" for name in sorted_drop_indexes.keys() if sorted_drop_indexes[name]!=1]
    # len_drop_index_stats = int(len(drop_index_stats)/2)
    len_drop_index_stats = len(drop_index_stats)
    
    if drop_index_stats != []: mv_index_stats = drop_index_stats[:len_drop_index_stats] + [index_info['index_stat'] for index_info in sorted_index_cnts]
    else : mv_index_stats = [index_info['index_stat'] for index_info in sorted_index_cnts]
    
    # logger.info(f"mv_index_stats total_length and sorted_length --> {len(all_indexes), len(mv_index_stats)} [{mv_index_stats}]")
    
    recom_indexes.append(mv_index_stats)
    
    logger.info(f"* The number of the recommendations is {len(recom_indexes)}")
    # logger.debug(f"* existing_indexes --> {existing_indexes, len(existing_indexes)}")
    # existing_indexes_default = existing_indexes
    
    recommend_tcost, recommend_constraint_sqls, total_used_indexes = multi_process_recom_indexes_estimation(recom_indexes, current_storage, storage_constraint, existing_indexes, workload, db_name, schema = schema)
    
    for i, recom_indexes_ in enumerate(recommend_constraint_sqls) :
        with open(detailed_info_dir + f"indexes/index_{iter_idx}th_{i}.sql", 'w') as file :
            for index in recom_indexes_ :
                file.write(index + '\n')

    # for idx, recom_index in enumerate(recom_indexes) :
    #     # logger.debug(f"* the {idx}th recom_index is {recom_index}")
    #     constraint_sqls, ex_indexes = hypopg_incremental_recommend_creation(conn, recom_index, current_storage, storage_constraint, existing_indexes, workload, schema)
    #     logger.debug(f"* constraint_sqls --> {constraint_sqls}")
    #     current_cost, used_indexes = query_plan_get_used_indexes(workload, conn, ex_indexes, schema)
    #     logger.info(f"* the {idx}th estimated cost is {current_cost}")
    #     hypopg_drop_indexes(conn, schema)
    #     current_storage, existing_indexes = hypopg_create_existing_indexes(conn, existing_indexes, schema)
    #     recommend_tcost.append(current_cost)
    #     recommend_constraint_sqls.append(constraint_sqls)
    #     total_used_indexes.append(used_indexes)

    current_best_cost = min(recommend_tcost)
    current_best_indexes = recom_indexes[recommend_tcost.index(current_best_cost)]
    current_best_indexes_constraint = recommend_constraint_sqls[recommend_tcost.index(current_best_cost)]
    current_best_used_indexes = total_used_indexes[recommend_tcost.index(current_best_cost)]
    logger.info(f"* current best estimated cost is {current_best_cost} [{recommend_tcost.index(current_best_cost)}]")
    
    return current_best_indexes_constraint, current_best_cost, current_best_used_indexes

def Actual_Exec(historical_info, historical_costs, historical_costs_str, args, logger) :    
    estimated_best = min(historical_costs)
    
    schema = args["schema"]

    logger.info("** Actual Execution Evaluation")
    drop_index_sql = f"select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = '{schema}';"
    drop_index_prefix(conn, drop_index_sql)
    
    num_of_actual_executions = args["num_of_actual_executions"]
    db_name = args["db_name"]
    bm_type = args["type"]
    if bm_type == "OLAP" : 
        detailed_info_dir = args["detailed_info_path"] + f"{db_name}_{int(index_storage_proportion * 100)}/"
        workload_path = args["AP_Config"]["workload_path"]
    else :
        benchmark = args["TP_Config"]["benchmark"] 
        workload_path = args["TP_Config"]["workload_path"]
        detailed_info_dir = args["detailed_info_path"] + f"{benchmark}_{int(index_storage_proportion * 100)}/"
    
    index_path = detailed_info_dir + "index.sql"
    
    # test execution
    if bm_type == 'OLAP' :
        _ = execute_sql_file(conn, workload_path)
    else :
        _ = oltp_stress_test(args['TP_Config']['benchmark'], args['TP_Config']['benchmark_config'])
    logger.info(f"* Finished the test case.")
    
    default_execution_time = 0
    historical_indexes = [info['best_indexes'] for info in historical_info]
    all_used_indexes = [info['used_indexes'] for info in historical_info]
    logger.info(f"* the historical_indexes --> {historical_indexes}")
    final_indexes = []
    
    for idx, indexes in enumerate(historical_indexes) :
        # drop_index_prefix(conn, drop_index_sql)
        avg_execution_time = 0
        if indexes != [] : # update all used indexes
            final_indexes = update_index_set(indexes, all_used_indexes[idx], final_indexes)   
        if idx ==0 or idx == historical_costs.index(estimated_best) :
            if idx == historical_costs.index(estimated_best) :
                execute_sql_index_creation(conn, final_indexes)
                logger.info(f"* current index definitions --> {final_indexes}, {len(final_indexes)}")
                with open(index_path, 'a') as file :
                    file.write(f"-- used indexes --> {list(all_used_indexes)[historical_costs.index(estimated_best)]}\n\n")
                    for index in final_indexes :
                        file.write(f"{index}\n")
                        
            ## workload execution
            reuslts_set = set()
            if bm_type == 'OLAP' :
                for i in range(num_of_actual_executions) :
                    logger.info(f"* the {i}th execution.")
                    if indexes == [] :
                        t = execute_sql_file(conn, workload_path)
                    else :
                        t = execute_sql_file_bar(conn, workload_path, default_execution_time)
                    reuslts_set.add(t)
            else :
                for i in range(num_of_actual_executions) :
                    logger.info(f"* the {i}th execution.")
                    result = oltp_stress_test(args['TP_Config']['benchmark'], args['TP_Config']['benchmark_config'])
                    reuslts_set.add(result)
        
            avg_result = statistics.median(reuslts_set)
            if indexes == [] : 
                default_result = avg_result
                logger.info(f"* the default execution time is {avg_result}.")
            if idx == historical_costs.index(estimated_best) : 
                logger.info(f"**** this is the estimated best result ****")
                logger.info(f"* the execution time of the best recommendation result in the {idx}th iteration is {avg_result}.")
                logger.info(f"* the cost reduction of the best recommendation result in the {idx}th iteration is {historical_costs_str[idx]}.")
                
                return
   
def What_If(historical_info, historical_costs, historical_costs_str, args, logger, conn) : 
    estimated_best = min(historical_costs)
    
    logger.info("** What-if Cost Evaluation")

    schema = args["schema"]
    
    if bm_type == "OLAP" : detailed_info_dir = args["detailed_info_path"] + f"{db_name}_{int(index_storage_proportion * 100)}/"
    else :
        benchmark = args["TP_Config"]["benchmark"] 
        detailed_info_dir = args["detailed_info_path"] + f"{benchmark}_{int(index_storage_proportion * 100)}/"
        
    index_path = detailed_info_dir + "index.sql"
    historical_indexes = [info['best_indexes'] for info in historical_info]
    all_used_indexes = [info['used_indexes'] for info in historical_info]
    # logger.info(f"* the historical_indexes --> {historical_indexes}")
    final_indexes = []
    
    if args["type"] == "OLAP" : workload_path = args["AP_Config"]["workload_path"]
    else : 
        benchmark = tp_config["benchmark"]
        workload_path = os.path.join(tp_config["workload_path"], f"{benchmark}.sql")
    
    with open(workload_path, "r") as f:
        workload = f.readlines()
    
    for idx, indexes in enumerate(historical_indexes) :
        avg_execution_time = 0
        if idx == 0 or idx == historical_costs.index(estimated_best) :
            if idx == 0 and indexes == [] : 
                def_cost, _ = query_plan_cost_estimation_used_indexes(workload, conn, schema)
                logger.info(f"* the default execution time is {historical_costs[0]} with execution cost {def_cost}.")
                continue

            if idx == historical_costs.index(estimated_best) :
                final_indexes = update_index_set(indexes, all_used_indexes[idx], final_indexes) 
                final_indexes.extend(indexes)
                logger.info(f"* current index definitions --> {final_indexes}, {len(final_indexes)}")

                # execute_sql_index_creation(conn, final_indexes)
                constraint_final_indexes = hypopg_indexes_creation_constraint(final_indexes, conn, schema)
                # assert len(final_indexes) == len(constraint_final_indexes), f"[What-if] Error final indexes (Exceeding storage constraint) {len(final_indexes)}, {len(constraint_final_indexes)}"
                if len(final_indexes) != len(constraint_final_indexes):
                    logger.error(f"[What-if] Error final indexes (Exceeding storage constraint) {len(final_indexes)}, {len(constraint_final_indexes)}")
                final_cost, used_indexes = query_plan_cost_estimation_used_indexes(workload, conn, schema)
                with open(index_path, 'a') as file :
                    file.write(f"-- used indexes --> {used_indexes}\n\n")
                    for index in constraint_final_indexes :
                        file.write(f"{index}\n")
                logger.info(f"**** this is the estimated best result ****")
                logger.info(f"* the execution time of the best recommendation result in the {idx}th iteration is {estimated_best}, with execution cost {final_cost}.")
                logger.info(f"* the cost reduction of the best recommendation result in the {idx}th iteration is {historical_costs_str[idx]}, with execution radio {((def_cost - final_cost) * 100) / def_cost}%.")
                
                return
        else :
            if indexes != [] :
                final_indexes = update_index_set(indexes, all_used_indexes[idx], final_indexes)                 
    
if __name__ == "__main__" :
    start_point = time.time()
    ## args
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path of config file", default="./config/config_gpt_tpch.json")
    config_path = parser.parse_args().config
    args = json.load(open(config_path, 'r'))

    ## definition
    db_name = args["db_name"]
    bm_type = args["type"]
    
    index_storage_proportion = args["index_storage_proportion"]
    num_of_iterations = args["num_of_iterations"]
    num_of_actual_executions = args["num_of_actual_executions"]
    num_of_samples = args["num_of_samples"]
    what_if = args['what_if']
    mode = args['mode']
    temperature = args["temperature"]
    model_name = args["model_name"]
    api_key = args["api_key"]
    demos_num = args['demos_num']

    schema = args['schema']
    
    if bm_type == "OLAP" : detailed_info_dir = args["detailed_info_path"] + f"{db_name}_{int(index_storage_proportion * 100)}/"
    else :
        benchmark = args["TP_Config"]["benchmark"] 
        detailed_info_dir = args["detailed_info_path"] + f"{benchmark}_{int(index_storage_proportion * 100)}/"
        
        oltp_stress_test_db(benchmark, args['TP_Config']['benchmark_config'])
    
    input_message = detailed_info_dir + "mes.txt"
    log_path = detailed_info_dir + "log.txt"
    index_path = detailed_info_dir + "index.sql"
    schema_path = f"./data/schema/{db_name}_schema.json"
    ndv_path = f"./data/ndv/{db_name}_ndv.json"
    output_path = detailed_info_dir + "indexes/"
    
    if not os.path.exists(detailed_info_dir) : os.makedirs(detailed_info_dir)
    if not os.path.exists(output_path) : os.makedirs(output_path) 
    
    # demos
    with open(args["demos_path"], 'r') as file :
        recommend_demos = json.load(file)
    
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
    
    with open(input_message, "w") as f:
        f.truncate()
    with open(index_path, 'w') as f :
        f.truncate()
    
    ## connect to remote database
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

    ## storage_constraint
    drop_index_sql = f"select indexname from pg_indexes where indexname not in (select conname from pg_constraint where contype = 'p') and schemaname = '{schema}';"
    drop_index_prefix(conn, drop_index_sql)
    hypopg_drop_indexes(conn, schema)

    set_db_point = time.time()
    ## index recommendation parameters
    starttime = time.time()
    
    cursor = conn.cursor()
    cursor.execute(f"select pg_database_size('{db_name}');")
    storage_constraint = (cursor.fetchone()[0] / ( 1024 * 1024 )) * index_storage_proportion
    conn.commit()
    cursor.close()
    
    ## with input information of SQLs, selectivity, NDV
    # workload_path + get_db_schema    
    if bm_type == "OLAP" :
        ap_config = args["AP_Config"]
        
        workload_path = ap_config["workload_path"]
        
        get_db_schema(db_name, schema_path, schema)
    elif bm_type == "OLTP" :
        tp_config = args["TP_Config"]
        
        benchmark = tp_config["benchmark"]
        schema_path = f"./data/schema/{benchmark}_schema.json"
        ndv_path = f"./data/ndv/{benchmark}_ndv.json"
        
        ## sample_path, weights
        workload_dir = tp_config["workload_dir"]
        
        workload_samples_paths = []
        for filename in os.listdir(workload_dir):
            if filename.startswith(benchmark) :
                workload_samples_paths.append(filename)
        workload_samples_paths = sorted(workload_samples_paths, key = extract_number)
        workload_samples_paths = [os.path.join(workload_dir, filename) for filename in workload_samples_paths]
        
        benchmark_config = tp_config["benchmark_config"]
        config_tree = etree.parse(benchmark_config)
        root = config_tree.getroot()
        
        weights_list = []
        for weights in root.findall(".//weights"):
            weights_text = weights.text.strip() 
            weights_list.extend(map(int, weights_text.split(",")))   
                
        ## workload gen
        workload = []
        workload_path = os.path.join(tp_config["workload_path"], f"{benchmark}.sql")
        
        # print(workload_samples_paths, weights_list)
        # exit()
        
        for sample_dir, weight in zip(workload_samples_paths, weights_list) :
            wg_files = [os.path.join(sample_dir, f) for f in os.listdir(sample_dir) if f.endswith('.wg')]
            wg_samples = random.sample(wg_files, weight)
            
            for wg in wg_samples :
                with open(wg, 'r') as file :
                    transaction = file.readlines()
                    workload.extend(transaction)
        
        random.shuffle(workload)
        
        with open(workload_path, 'w') as file :
            for sql in workload :
                file.write(sql)        
                
        get_db_schema(db_name, schema_path, schema)
    else :
        print(f"Error type in {bm_type}")
        exit()
    
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
    
    workload_infos, used_column_info = obtain_workload_information(workload, conn, db_name, schema_path, schema = schema)
    # print(len(list(workload_infos['where_selectivities'].keys())))
    # exit()

    for column in used_column_info :
        only_column = column[0].split('.')[1].split(':')[0]
        table_column = column[0].split(':')[0].strip()
        column_type = column[0].split(':')[1].strip()
        only_column_names.append(only_column)
        column_types.append(column_type)
        table_columns.append(table_column)
        column_rows.append(column[1])
    table_column_rows = {k : v for k, v in zip(table_columns, column_rows)}
    table_rows = dict(list(set([(k.split('.')[0], v) for k, v in table_column_rows.items()])))
    
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
        if ndvs == {} :
            ndvs = get_ndvs_all(conn, ndv_path, schema)
    else :
        ndvs = get_ndvs_all(conn, ndv_path, schema) # ratio
        
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
    existing_indexes_keys, index_infos = obtain_default_index_statements(db_name, schema_path, schema) # only consider the default indexes contains pk
    existing_indexes = {}
    for key in existing_indexes_keys :
        existing_indexes[key] = 0
    current_storage = 0
    
    ## GPT Recommendation with iterations      
    historical_info = []
    historical_costs_str = []
    historical_costs = []
    best_indexes = []
    # default
    default_cost, used_indexes = query_plan_get_used_indexes(workload, conn, existing_indexes_keys, schema)
    historical_info.append({"best_indexes" : best_indexes, "used_indexes" : used_indexes})
    historical_costs_str.append("default cost")
    historical_costs.append(default_cost)
    history = dict(zip(historical_costs_str, historical_info))
    
    ## aggregate same sql info
    sql_columns_agg = {}
    sql_id = 0
    for sql, columns in sql_columns.items() :
        if columns in [v['Columns'] for v in sql_columns_agg.values()] :
            for sql_, columns_ in sql_columns_agg.items() :
                if columns == columns_['Columns'] : 
                    sql_columns_agg[sql_]["Counts"] += 1
                    break
        else :
            sql_columns_agg[f"SQL_{sql_id}"] = {'Columns' : columns, 'Counts' : 1}
            sql_id += 1
    sql_columns_agg = dict(sorted(sql_columns_agg.items(), key=lambda x: -x[1]['Counts']))
    if list(set([v['Counts'] for v in sql_columns_agg.values()])) == [1] : sql_columns_agg = sql_columns
    
    
    # input_info = {'sorted_used_column_cnts_ndvs' : sorted_used_column_cnts_ndvs, 'sorted_where_selectivities' : sorted_where_selectivities, 'where_cols_selectivities' : sorted_where_cols_selectivities, 'sorted_other_predicates' : sorted_other_predicates, 'sorted_group_order_columns' : sorted_group_order_columns, 'sql_columns' : sql_columns, 'existing_indexes' : index_infos, 'remain_avail_storage' : f"{round((storage_constraint - current_storage) * 100 / storage_constraint, 4)}%" , 'history' : history} 
    input_info = {'sorted_used_column_cnts_ndvs' : sorted_used_column_cnts_ndvs, 'sql_columns' : sql_columns_agg, 'table_rows' : table_rows, 'sorted_where_selectivities' : sorted_where_selectivities, 'where_cols_selectivities' : sorted_where_cols_selectivities,'sorted_other_predicates' : sorted_other_predicates, 'sorted_group_order_columns' : sorted_group_order_columns, 'existing_indexes' : existing_indexes, 'remain_avail_storage' : f"{int(storage_constraint - current_storage)}" , 'history' : history, 'workload' : workload } # 'workload':workload,'remain_avail_storage' : f"{round((storage_constraint - current_storage) * 100 / storage_constraint, 4)}%"
    logger.info("-----------------------------------------------------------------------------")
    # print(input_info["sorted_used_column_cnts_ndvs"])
    # exit()
    
    ## save the results to the output_path    
    shutil.rmtree(output_path)
    os.mkdir(output_path)
    
    logger.info(f"* model is {model_name}, with temperature as {temperature}, num_of_samples as {num_of_samples}")
    logger.info(f"* mode is {mode}")
    system_mes = ''
    terminal_cnt = 0
    last_cost = default_cost
    best_indexes = []
    best_used_indexes = []
    
    len_demos = len(recommend_demos)
    
    # for key, value in input_info.items() :
    #     print(f"{key} : {value}")
    # exit()
    
    # seed
    # random.seed(10)
    
    for iter_idx in range(num_of_iterations) :
        # # input --> recom_indexes
        # if what_if : recom_indexes, finished = GPT_whatif(args, input_info, recommend_demos, iter_idx, logger)
        # else : recom_indexes, finished = GPT_lat(args, input_info, recommend_demos, iter_idx, logger)
        
        # if finished : break
        
        recom_indexes, finished = GPT_whatif(args, input_info, recommend_demos, iter_idx, logger)
        
        if finished : break
                
        # recom_indexes, info --> current_best_cost, best_indexes, best_used_indexes
        if mode == "what_if" :
            recom_indexes, recom_cost, recom_used_indexes = CM_what_if(recom_indexes, current_storage, storage_constraint, existing_indexes, schema)
            
        elif mode == "major_voting" :
            recom_indexes, recom_cost, recom_used_indexes = CM_major_voting(recom_indexes, current_storage, storage_constraint, existing_indexes, schema)
            
        elif mode == "index_infer" : 
            recom_indexes, recom_cost, recom_used_indexes = CM_index_infer(recom_indexes, current_storage, storage_constraint, existing_indexes, db_name, args, iter_idx)
        else : 
            logger.error(f"Error in the selection of \"mode\" in the configuration file, which should be set in "what_if", "major_voting", and "index_infer"!")
            exit()
                     
            
        keep_ex_indexes = False
        if recom_cost == last_cost :
            terminal_cnt += 1
        else :
            if recom_cost > last_cost :
                keep_ex_indexes = True
            last_cost = recom_cost
            terminal_cnt = 0
                
        if terminal_cnt >= 3 : 
            logger.info("* there are no more improvement, so the recommendation has finished *")
            break
        
        historical_info.append({"best_indexes" : recom_indexes, "used_indexes" : recom_used_indexes})
        historical_costs.append(recom_cost)
        # logger.info(f"* used indexes + existing indexes / recommended indexes --> [{len(recom_used_indexes)} + {len(existing_indexes)} / {len(recom_indexes)}]")
        
        # drop redundant indexes and update existing indexes
        if not keep_ex_indexes :
            best_indexes = recom_indexes # reserve current best cost even worse than before
            best_used_indexes = list(recom_used_indexes)
        else :
            logger.debug(f"* the recommended result is not better than before --> keep used_indexes : {best_used_indexes}")  
            
        historical_costs_str = write_history_cost_str(historical_info, historical_costs, historical_costs_str)
        history = dict(zip(historical_costs_str, historical_info))

        existing_indexes, current_storage = hypopg_update_used_indexes(conn, recom_used_indexes, existing_indexes, recom_indexes, schema)
        # logger.info("===========================================")
        # logger.info(f"* [Updated] existing_indexes --> {existing_indexes}")
        # exit()
        # logger.info(f"* [Updated] existing indexes --> {len(existing_indexes)}")
        # logger.debug(f"* [Updating current iteration recommendation] existing_indexes_length, current_storage_proportion --> {len(existing_indexes), float(current_storage / storage_constraint)}")
        
        input_info['history'] = history
        input_info['remain_avail_storage'] = f"{int(storage_constraint - current_storage)}"
        input_info['existing_indexes'] = list(existing_indexes.keys())
        
        
    logger.info(f"* historical_info recommendation length : {[len(historical_index) for historical_index in historical_info]}")
    logger.info(f"* historical_costs fluctuation : {historical_costs_str}")
    stoptime = time.time()
    logger.info(f"* the recommendation time is {stoptime - starttime}.")
    logger.info("-----------------------------------------------------------------------------")
    
    ## Actual Execution Evaluation
    drop_index_prefix(conn, drop_index_sql)
    hypopg_drop_indexes(conn, schema)
    if what_if :
        What_If(historical_info, historical_costs, historical_costs_str, args, logger, conn)
    else :
        Actual_Exec(historical_info, historical_costs, historical_costs_str, args, logger)
        
    logger.info("** Finished!")