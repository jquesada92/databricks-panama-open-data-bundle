from utils import (
    CATALOG,
    SCHEMA_REFERENCE_AUDIT,
    SCHEMA_PAYROLL,
    TABLE_REFERENCE_STATUS,
    TABLE_REFERENCE_INSTITUCIONS,
    TABLE_AUDIT_API_CHECK,
) 

SETUP_QUERY = f""" 
               
CREATE CATALOG IF NOT EXISTS {CATALOG};
CREATE SCHEMA IF NOT EXISTS {SCHEMA_REFERENCE_AUDIT};
CREATE SCHEMA IF NOT EXISTS {SCHEMA_PAYROLL};



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
"""
list(map(spark.sql, filter(lambda x: x.strip() != "", SETUP_QUERY.split(";"))))



from utils import Contraloria

__contraloria = Contraloria()
__contraloria.read_update_date()
query_date = __contraloria.get_query_date()

status_lst = __contraloria.get_status_list()
institution_lst = __contraloria.get_institution_list()
last_update_date = __contraloria.get_update_date()


from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime as dt 

up_to_date_df = spark.read.table(TABLE_AUDIT_API_CHECK).where(f'run_status =="OK" AND source_update >= "{last_update_date}" ')


from itertools import product
save_path =  '/Volumes/contraloria/reference_and_audit/contraloria_staging'
dbutils.fs.mkdirs(save_path)
updates = 0

for institution, status in product(institution_lst, status_lst):
    e = 'OK'
    rsp = 'No Update'
    start = dt.now()
    if up_to_date_df.where(f"institution_name_spanish = '{institution}' AND status_name_spanish = '{status}'").isEmpty(): 
        try:
        
            __contraloria.download_report(institution,status, save_path)
            rsp = 'OK'
            updates += 1

        except Exception as err:
            e = err
            rsp = 'FAIL'
    else:
        e = 'No Updates'
    end = dt.now()
        
    print(f"""{institution} - {status} -> {e}""")

    spark.sql(f"""
        INSERT INTO {TABLE_AUDIT_API_CHECK}
        VALUES ('{institution}','{status}','{rsp}','{last_update_date}','{query_date}','{start}','{end}','{(end - start).total_seconds()}')
    """)


dbutils.jobs.taskValues.set(key="updates", value=updates)


