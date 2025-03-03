## Random select sqls to form the test workload file
## Generate one workload file to multi single sql files into a workload directory

import os
import re
import json
import random 
import glob
import shutil

def load_data(path) :
    with open(path) as file :
        queries = file.readlines()
    process_queries = []
    for query in queries :
        query = query.strip().replace("\n", " ")
        query = re.sub(r"\s+", " ", query)
        process_queries.append(query+'\n')
    
    return process_queries

def generate_workload_files(queries, num, output_dir) :
    if not os.path.exists(output_dir) :
        os.mkdir(output_dir)
    
    for i in range(num) :
        queries_cnt = random.randint(10,100)
        workload = random.sample(queries, queries_cnt)
        with open(f"{output_dir}/workload_{i}.sql", 'w') as file :
            file.truncate()
        with open(f"{output_dir}/workload_{i}.sql", 'a') as file :
            for query in workload :
                file.write(query)
    
if __name__ == "__main__" :
    orgin_path = "" # .sql
    output_dir = ""
    num = 0
    
    queries = load_data(orgin_path)
    generate_workload_files(queries, num, output_dir)