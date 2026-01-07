import subprocess
import os
import time
import json

tmpdir = os.getenv('TMP', "/tmp")

# READ CONFIG FROM config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# EXTRACT DATABASE CONFIG
db_config = config['database']
dbhost = db_config['host']
dbname = db_config['db_name']
dbuser = db_config['username']
dbpass = db_config['password']
dbport = 5432

# EXTRACT HAMMERDB CONFIG
hammerdb_config = config['hammerdb']

# EXTRACT CASE CONFIG 
case = config['cases'][0]
mw_vu = case.get('mw_vu', 0.6)

dbset('db','pg')
dbset('bm','TPC-C')
dbset('vindex', case.get('vindex', 'hnsw'))

diset('connection','pg_host', dbhost)
diset('connection','pg_port', dbport)
diset('connection','pg_sslmode','prefer')

diset('tpcc','pg_superuser', dbuser)
diset('tpcc','pg_superuserpass', dbpass)
diset('tpcc','pg_defaultdbase', dbname)
diset('tpcc','pg_user', dbuser)
diset('tpcc','pg_pass', dbpass)
diset('tpcc','pg_dbase', dbname)
diset('tpcc','pg_driver', hammerdb_config.get('pg_driver', 'timed'))
diset('tpcc','pg_total_iterations', hammerdb_config.get('pg_total_iterations', '10000000'))
diset('tpcc','pg_rampup', hammerdb_config.get('pg_rampup', '1'))
diset('tpcc','pg_duration', hammerdb_config.get('pg_duration', '1'))
diset('tpcc','pg_allwarehouse', hammerdb_config.get('pg_allwarehouse', 'false'))
diset('tpcc','pg_timeprofile', hammerdb_config.get('pg_timeprofile', 'true'))
diset('tpcc','pg_vacuum', hammerdb_config.get('pg_vacuum', 'false'))
giset("commandline", "keepalive_margin", hammerdb_config.get('keepalive_margin', '90'))

print(f"DEBUG: About to load vector data, mw_vu from config={mw_vu}")

print("STARTED LOADING VECTOR DATA IN DB AND BUILDING INDEX")
result = subprocess.run(["vectordbbench", "pgvectorhnsw", "--config-file", "/home/emumba/emumba/VDB/VectorDBBench/vectordb_bench/config-files/sample_config.yml"], capture_output=True)
print(result)
print("VECTOR DATA LOADED AND INDEX BUILD COMPLETE")

if result.returncode == 0:
    buildschema()
    
    # SET mw_vu RIGHT BEFORE loadscript()
    print(f"DEBUG: Setting mw_vu={mw_vu} in HammerDB before loadscript()")
    dvset("mixed_workload", "mw_vu", str(mw_vu))
    
    loadscript()
    vudestroy()
    print("TEST STARTED")
    
    # READ VU COUNT FROM CONFIG
    vu_count = case.get('num_concurrency', [4])[0] if isinstance(case.get('num_concurrency'), list) else case.get('num_concurrency', 4)
    vuset('vu', str(vu_count))
    
    vucreate()
    tcstart()
    tcstatus()
    jobid = tclpy.eval('vurun')
    vudestroy()
    tcstop()
    print("TEST COMPLETE")
    file_path = os.path.join(tmpdir, "pg_tprocc")
    fd = open(file_path, "w")
    fd.write(jobid)
    fd.close()
    time.sleep(10)

    print("STARTING RECALL CALCULATION")
    diset('tpcc','pg_driver','test')
    customscript("recall_calculation.tcl")
    vuset("vu", "1")
    vucreate()
    tcstart()
    tcstatus()
    jobid = tclpy.eval('vurun')
    vudestroy()
    tcstop()
    print("TEST COMPLETE")
    file_path = os.path.join(tmpdir, "pg_tprocc")
    fd = open(file_path, "w")
    fd.write(jobid)
    fd.close()
    print("RECALL CALCULATION COMPLETE")
exit()