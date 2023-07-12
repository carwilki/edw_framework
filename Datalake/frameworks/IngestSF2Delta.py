import argparse
from Datalake.utils.DeltaLakeWriter import DeltaLakeWriter
import json

parser = argparse.ArgumentParser()

parser.add_argument("env", type=str, help="Env Variable")
parser.add_argument("sf_database", type=str, help="sf_database Variable")
parser.add_argument("sf_schema", type=str, help="sf_schema Variable")
parser.add_argument("table_list", type=str, help="list of tables")
args = parser.parse_args()
env = args.env
sf_database = args.sf_database
sf_schema = args.sf_schema
table_list = args.table_list
table_list = [table for table in args.table_list.split(",")]
table_list = json.dumps(table_list)

for table in table_list:
    DeltaLakeWriter(env, sf_database, sf_schema, table).ingestFromSF(
        env, sf_database, sf_schema, table_list
    )
