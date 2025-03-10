import json
from utils.db_utils import get_table_names_from_db, execute_sql, get_db_info

# Extract schema to json[with db_name]

db_name = "tpch"

print(db_name)
data_types = []
table_names = get_table_names_from_db(db_name)

create_table_statements, primary_key_statements, foreign_key_statements, create_index_statements = get_db_info(db_name, table_names)

db_schema = dict() # schema
table_info_list = []
for table_name in table_names:
    table_info = dict()
    table_info["table"] = table_name
    rows = execute_sql(f"SELECT COUNT(*) FROM {table_name};", db_name)[0][0]
    table_info["rows"] = rows
    table_info["columns"] = []
    columns_and_types = execute_sql(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';", db_name)
    for (column, data_type) in columns_and_types:
        data_type = data_type.upper()
        
        table_info["columns"].append(
            {
                "name": column,
                "type": data_type
            }
        )
        data_types.append(data_type)
    
    table_ddl = None
    for ddl in create_table_statements:
        if "CREATE TABLE {} (".format(table_name).lower() in ddl.lower():
            table_ddl = ddl
            break
    
    if table_ddl is None:
        raise ValueError("can not find ddl for table {}".format(table_name))

    table_info["ddl"] = table_ddl

    table_info_list.append(table_info)

db_schema["table_info"] = table_info_list
db_schema["primary_key_statements"] = primary_key_statements
db_schema["foreign_key_statements"] = foreign_key_statements
db_schema["create_index_statements"] = create_index_statements

print(set(data_types))
print(len(set(data_types)))

with open(f"./data/{db_name}_test_schema.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(db_schema, indent=2, ensure_ascii=False))