import json
from utils.db_utils import execute_sql
import sqlparse

# Pruning schema with given sql

if __name__ == "__main__":
    db_name = "tpch"

    sql = '''select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
    from
            lineitem
    where
            l_shipdate <= date '1998-12-01' - interval '74' day
    group by
            l_returnflag,
            l_linestatus
    order by
            l_returnflag,
            l_linestatus;'''
    schema = json.load(open("/home/lihaoyang/ai4db-llm/data/db_schema/{}_schema.json".format(db_name)))
    sql = sqlparse.format(sql, keyword_case = 'upper', identifier_case = 'lower')
    used_table_names = []
    used_column_info = []
    used_table_ddls = []
    
    for table in schema["table_info"]:
        if table["table"].lower() in sql.lower():
            used_table_names.append(table["table"].lower())
            used_table_ddls.append(table["ddl"])
            for column in table["columns"]:
                if column["name"].lower() in sql.lower():
                    used_column_info.append(table["table"].lower() + "." + column["name"].lower() + ": " + column["type"])

    used_pk_stmts, used_fk_stmts, used_index_stmts = [], [], []
    
    for primary_key_statement in schema["primary_key_statements"]:
        for table_name in used_table_names:
            if "ALTER TABLE ONLY {}\n".format(table_name).lower() in primary_key_statement.lower() and primary_key_statement not in used_pk_stmts:
                used_pk_stmts.append(primary_key_statement)

    for foreign_key_statement in schema["foreign_key_statements"]:
        fk_source_table_name = foreign_key_statement.replace("ALTER TABLE ONLY ", "").split("\n")[0].strip()
        fk_target_table_name = foreign_key_statement.split("REFERENCES")[1].split("(")[0].strip()
        if fk_source_table_name.lower() in used_table_names and fk_target_table_name.lower() in used_table_names and foreign_key_statement not in used_fk_stmts:
            used_fk_stmts.append(foreign_key_statement)
    
    for create_index_statement in schema["create_index_statements"]:
        for table_name in used_table_names:
            if "ON {} USING".format(table_name).lower() in create_index_statement.lower() and create_index_statement not in used_index_stmts:
                used_index_stmts.append(create_index_statement)
    
    execution_results = execute_sql("EXPLAIN {}".format(sql), db_name)
    query_plan = "\n".join([row[0] for row in execution_results])
    print(type(query_plan))

    pk = "\n".join(used_pk_stmts) if len(used_pk_stmts) != 0 else "None"
    fk = "\n".join(used_fk_stmts) if len(used_fk_stmts) != 0 else "None"
    indexes = "\n".join(used_index_stmts) if len(used_index_stmts) != 0 else "None"

    print("\n".join(used_column_info))
    print()
    print("\n".join(used_table_ddls))
    print(query_plan)
    print()
    print(pk)
    print()
    print(fk)
    print()
    print(indexes)
    print()
    print(sql)