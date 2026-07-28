from contraloria import Contraloria
from datetime import datetime as dt
from itertools import product
import argparse


parser = argparse.ArgumentParser(description="configuracion de schema")
parser.add_argument("--catalog", type=str, required=True, help="Nombre del catálogo")
parser.add_argument("--schema", type=str, required=True, help="Nombre del schema")
parser.add_argument("--volume", type=str, required=True, help="Nombre del volume")

args = parser.parse_args()

catalog = args.catalog
schema = args.schema
volume = args.volume


TABLE_AUDIT_API_CHECK = f"{catalog}.{schema}.control_de_actualizaciones_contraloria"

spark.sql(f"""     
CREATE TABLE IF NOT EXISTS {TABLE_AUDIT_API_CHECK} (
    institution_name_spanish STRING NOT NULL,
    status_name_spanish STRING NOT NULL,
    run_status STRING NOT NULL,
    source_update TIMESTAMP NOT NULL,
    checked_at TIMESTAMP DEFAULT current_timestamp(),
    start_at TIMESTAMP NOT NULL,
    end_at TIMESTAMP NOT NULL,
    time FLOAT NOT NULL
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
""")


__contraloria = Contraloria()
__contraloria.read_update_date()
query_date = __contraloria.get_query_date()

status_lst = __contraloria.get_status_list()
institution_lst = __contraloria.get_institution_list()
last_update_date = __contraloria.get_update_date()


up_to_date_df = spark.read.table(TABLE_AUDIT_API_CHECK).where(
    f'run_status =="OK" AND source_update >= "{last_update_date}" '
)


save_path = f"/Volumes/{catalog}/{schema}/{volume}/data"
dbutils.fs.mkdirs(save_path)

updates = 0

for institution, status in product(institution_lst, status_lst):
    e = "OK"
    rsp = "No Update"
    start = dt.now()
    if up_to_date_df.where(
        f"institution_name_spanish = '{institution}' AND status_name_spanish = '{status}'"
    ).isEmpty():
        try:
            __contraloria.download_report(institution, status, save_path)
            rsp = "OK"
            updates += 1

        except Exception as err:
            e = err
            rsp = "FAIL"
    else:
        e = "No Updates"
    end = dt.now()

    print(f"""{institution} - {status} -> {e}""")

    spark.sql(f"""
        INSERT INTO {TABLE_AUDIT_API_CHECK}
        VALUES ('{institution}','{status}','{rsp}','{last_update_date}','{query_date}','{start}','{end}','{(end - start).total_seconds()}')
    """)


dbutils.jobs.taskValues.set(key="updates", value=updates)
