import json

def load_json(path) :
    with open(path, 'r') as file :
        content = json.load(file)
    return content

def write_json(data, path) :
    with open(path, 'w') as file :
        json.dump(data, file, indent=4)

if __name__ == "__main__" :    
    exec_json = ""
    ref_json = ""
    output_path = ""
    
    exec_demos = load_json(exec_json)
    ref_demos = load_json(ref_json)
    
    dict_list = [exec_demos, ref_demos]
    
    output_demos = min(dict_list, key=len).copy()
    
    for key, _ in output_demos.items() :
        output_demos[key] = exec_demos[key].copy()

                
        # storage constraint
        for storage_constraint in output_demos[key] :
            if '%' in storage_constraint :
                output_demos[key][storage_constraint] = {}
                
                refine_demos = []
                for demo in ref_demos[key][storage_constraint]["demos"] :
                    demo_ = demo.copy()
                    def_cost = demo['default_cost']
                    ref_cost = exec_demos[key][storage_constraint]["recommended_cost"]
                    if def_cost > ref_cost :
                        demo_["recom_cost_fluctuation"] = f"Reduce {(def_cost - ref_cost)*100/def_cost}%"
                    else :
                        demo_["recom_cost_fluctuation"] = f"Increase {(ref_cost - def_cost)*100/def_cost}%"
                    refine_demos.append(demo_)
                # refine results
                output_demos[key][storage_constraint]['refine_demos'] = refine_demos
                
                # best results
                output_demos[key][storage_constraint]['best_recommended_indexes'] = exec_demos[key][storage_constraint]["recommended_indexes"]
                output_demos[key][storage_constraint]['best_used_indexes'] = exec_demos[key][storage_constraint]["recommended_used_indexes"]
                output_demos[key][storage_constraint]['best_cost'] = exec_demos[key][storage_constraint]["recommended_cost"]
                output_demos[key][storage_constraint]['best_cost_fluctuation'] = exec_demos[key][storage_constraint]["cost_fluctuation"]
                    
    print(len(output_demos.keys()))
    write_json(output_demos, output_path)
    