import json
import subprocess
import numpy as np

## indexes generation based on heuristic methods

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def write_json(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)


db_name = ''
config_json_file = ''
cmd = ['python', '-m', 'selection', config_json_file]

start_n = 0
end_n = 200
heuristics_methods_dir_cmd = "cd [dir_of_magicmirror]"

subprocess.run(heuristics_methods_dir_cmd, shell=True, capture_output=True, text=True)

for i in np.arange(start_n, end_n, 1) :
# for i in ar :
    print(f"** this is the {i}th execution **")
    config = load_json(config_json_file)
    config["benchmark_name"] = f"{db_name}_workload_{i}"
    write_json(config_json_file, config)
    
    subprocess.run(cmd)
    print(f"* Run completed for {config['benchmark_name']} *")
