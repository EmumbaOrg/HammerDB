"""
PostgreSQL monitoring functions for cache, index hits, and page activity tracking.
"""

import os
import time
import psycopg2
from datetime import datetime
from multiprocessing import Event


def get_query_by_description(description: str) -> str | None:
    """Load a specific query from queries.json by its description."""
    import json
    try:
        with open('queries.json', 'r') as file:
            queries = json.load(file)
        
        for item in queries:
            if item['description'] == description:
                return item['query']
        
        print(f"Warning: Query with description '{description}' not found in queries.json")
        return None
    except Exception as e:
        print(f"Failed to load query from queries.json: {e}")
        return None


def monitor_buffercache(db_config: dict, output_dir: str, interval_seconds: int, stop_event: Event):
    """
    Continuously monitor PostgreSQL buffer cache and write usage stats to a CSV file at precise intervals.
    
    Args:
        db_config: Database connection configuration
        output_dir: Directory to write CSV output file
        interval_seconds: Seconds between monitoring snapshots
        stop_event: Event to signal monitoring should stop
    """
    
    def now_ts():
        """Get current timestamp in precise format."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    csv_file_path = os.path.join(output_dir, "cache_monitoring.csv")

    buffercache_query = get_query_by_description("Buffer Usage from pg_buffercache")
    if buffercache_query is None:
        print("ERROR: Could not load buffer cache query from queries.json")
        return

    try:
        with psycopg2.connect(
            dbname=db_config['db_name'],
            user=db_config['username'],
            password=db_config['password'],
            host=db_config['host']
        ) as conn:
            conn.autocommit = True

            with conn.cursor() as cur:
                with open(csv_file_path, 'w') as csv_file:
                    csv_file.write("timestamp,used,empty,total,percent\n")
                    csv_file.flush()

                    next_run = time.monotonic()

                    while not stop_event.is_set():
                        ts = now_ts()
                        try:
                            cur.execute(buffercache_query)
                            result = cur.fetchone()
                            if result:
                                used, empty, total, percent = result
                                csv_file.write(f"{ts},{used},{empty},{total},{percent}\n")
                            else:
                                csv_file.write(f"{ts},ERROR,ERROR,ERROR,ERROR\n")
                        except Exception as e:
                            csv_file.write(f"{ts},ERROR,ERROR,ERROR,ERROR\n")

                        csv_file.flush()

                        next_run += interval_seconds
                        while True:
                            remaining = next_run - time.monotonic()
                            if remaining <= 0 or stop_event.is_set():
                                break
                            time.sleep(min(1, remaining))

    except Exception as e:
        print(f"[CACHE_MONITOR] Failed: {e}")


def monitor_index_hits(db_config: dict, output_dir: str, interval_seconds: int, stop_event: Event):
    """
    Monitor PostgreSQL index cache hits/misses and write stats to a CSV file at precise intervals.
    """
    
    def now_ts():
        """Get current timestamp in precise format."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    csv_file_path = os.path.join(output_dir, "index_hits_monitoring.csv")
    
    index_hits_query = get_query_by_description("Index Hit Ratio with Table and Index Names")
    if index_hits_query is None:
        print("ERROR: Could not load index hits query from queries.json")
        return
    
    try:
        with psycopg2.connect(
            dbname=db_config['db_name'],
            user=db_config['username'],
            password=db_config['password'],
            host=db_config['host']
        ) as conn:
            conn.autocommit = True
            
            with conn.cursor() as cur:
                with open(csv_file_path, 'w') as csv_file:
                    csv_file.write("timestamp,schema,table_name,index_name,index_hit_ratio\n")
                    csv_file.flush()
                    
                    next_run = time.monotonic()
                    
                    while not stop_event.is_set():
                        ts = now_ts()
                        try:
                            cur.execute(index_hits_query)
                            results = cur.fetchall()
                            
                            if results:
                                for row in results:
                                    schema, table_name, index_name, hit_ratio = row
                                    hit_ratio_str = str(hit_ratio) if hit_ratio is not None else "NULL"
                                    csv_file.write(f"{ts},{schema},{table_name},{index_name},{hit_ratio_str}\n")
                            else:
                                csv_file.write(f"{ts},NO_DATA,NO_DATA,NO_DATA,NO_DATA\n")
                        
                        except Exception as e:
                            csv_file.write(f"{ts},ERROR,ERROR,ERROR,ERROR\n")
                        
                        csv_file.flush()
                        
                        next_run += interval_seconds
                        while True:
                            remaining = next_run - time.monotonic()
                            if remaining <= 0 or stop_event.is_set():
                                break
                            time.sleep(min(1, remaining))
    
    except Exception as e:
        print(f"[INDEX_HITS_MONITOR] Failed: {e}")


def monitor_page_activity(db_config: dict, output_dir: str, interval_seconds: int, stop_event: Event):
    """
    Monitor page loads/removals by tracking buffer count changes per table/index.
    Uses pg_buffercache to count buffers, then calculates deltas to detect:
    - Positive delta = pages loaded into cache
    - Negative delta = pages evicted from cache
    """
    
    def now_ts():
        """Get current timestamp in precise format."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    csv_file_path = os.path.join(output_dir, "page_activity_monitoring.csv")
    
    page_activity_query = """
    SELECT 
        n.nspname AS schema,
        c.relname AS relation,
        CASE c.relkind 
            WHEN 'r' THEN 'table'
            WHEN 'i' THEN 'index'
            WHEN 't' THEN 'toast'
            WHEN 'm' THEN 'matview'
            ELSE 'other'
        END AS relation_type,
        count(*) AS buffer_count,
        pg_size_pretty(count(*) * 8192) AS cache_size
    FROM pg_buffercache b
    JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid) 
        AND b.reldatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname NOT LIKE 'pg_toast%'
    GROUP BY n.nspname, c.relname, c.relkind
    HAVING count(*) > 0
    ORDER BY count(*) DESC;
    """
    
    prev_buffer_counts = {}
    
    try:
        with psycopg2.connect(
            dbname=db_config['db_name'],
            user=db_config['username'],
            password=db_config['password'],
            host=db_config['host']
        ) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                with open(csv_file_path, 'w') as csv_file:
                    csv_file.write("timestamp,schema,relation,relation_type,buffer_count,buffer_delta,cache_size,activity\n")
                    csv_file.flush()
                    
                    next_run = time.monotonic()
                    while not stop_event.is_set():
                        ts = now_ts()
                        try:
                            cur.execute(page_activity_query)
                            results = cur.fetchall()
                            
                            current_buffer_counts = {}
                            
                            if results:
                                for row in results:
                                    schema, relation, rel_type, buffer_count, cache_size = row
                                    
                                    key = (schema, relation)
                                    current_buffer_counts[key] = buffer_count
                                    
                                    prev_count = prev_buffer_counts.get(key, buffer_count)
                                    buffer_delta = buffer_count - prev_count
                                    
                                    if buffer_delta > 0:
                                        activity = "LOADED"
                                    elif buffer_delta < 0:
                                        activity = "EVICTED"
                                    else:
                                        activity = "STABLE"
                                    
                                    csv_file.write(f"{ts},{schema},{relation},{rel_type},{buffer_count},{buffer_delta},{cache_size},{activity}\n")
                                
                                for key, prev_count in prev_buffer_counts.items():
                                    if key not in current_buffer_counts:
                                        schema, relation = key
                                        buffer_delta = -prev_count
                                        csv_file.write(f"{ts},{schema},{relation},unknown,0,{buffer_delta},0 bytes,FULLY_EVICTED\n")
                                
                                prev_buffer_counts = current_buffer_counts
                            else:
                                csv_file.write(f"{ts},NO_DATA,NO_DATA,NO_DATA,0,0,0 bytes,NO_DATA\n")
                            
                        except Exception as e:
                            print(f"[PAGE_ACTIVITY_MONITOR] Error: {e}")
                            csv_file.write(f"{ts},ERROR,ERROR,ERROR,0,0,0 bytes,ERROR\n")
                        
                        csv_file.flush()
                        
                        next_run += interval_seconds
                        while True:
                            remaining = next_run - time.monotonic()
                            if remaining <= 0 or stop_event.is_set():
                                break
                            time.sleep(min(1, remaining))
    
    except Exception as e:
        print(f"[PAGE_ACTIVITY_MONITOR] Failed: {e}")