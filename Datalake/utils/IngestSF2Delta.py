import argparse
from Datalake.utils.DeltaLakeWriter import DeltaLakeWriter
from Datalake.utils.genericUtilities import getSfCredentials


parser = argparse.ArgumentParser()

parser.add_argument("env", type=str, help="Env Variable")
parser.add_argument("table_list", type=str, help="list of tables")
args = parser.parse_args()
env = args.env
table_list = args.table_list
table_list = [table for table in args.table_list.split(",")]

# table_list = json.dumps(table_list)

sfOptions = getSfCredentials(env)

for table in table_list:
    print(table)
    delta_schema = table.split(".")[0]
    tableName = table.split(".")[1]
    DeltaLakeWriter(sfOptions, tableName, delta_schema).pull_data()
