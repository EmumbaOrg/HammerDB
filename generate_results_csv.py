"""
Automated CSV Generation Script for Mixed Workload Benchmark Results
=====================================================================

PURPOSE:
    Extracts benchmark metrics from HammerDB mixed workload test results and 
    generates CSV files for analysis.

WHAT IT EXTRACTS:
    From hdbxtprofile.log:
        - Vector VU count (counted from SEMANTIC_SEARCH sections)
        - OLTP VU count (calculated: Total VU - Vector VU)
        - NOPM 
        - QPS 
    
    From log.txt:
        - Test parameters (m, ef-construction, maintenance-work-mem, etc.)
        - Used for case matching
    
    From directory name:
        - Run count (e.g., 0, 1, 2 from folder name pattern: *-{run}-{random})
    
    From config.json:
        - vindex (hnsw, hnsw_bq, diskann) → mapped to extension names
        - Multiple test cases configurations

HOW IT WORKS:
    1. Walks through results/ directory recursively
    2. Finds folders containing: hdbxtprofile.log + config.json + log.txt
    3. Extracts ALL SUMMARY sections (one per num-concurrency value)
    4. Matches each test to correct case in config.json using parameter scoring
    5. Groups results by db-label
    6. Generates separate CSV per db-label (or combined CSV if configured)

CASE MATCHING:
    When config.json has multiple cases with **same db-label**:
    - Extracts test parameters from log.txt
    - Scores each case based on parameter matches:
        • m value 
        • ef-construction 
        • maintenance-work-mem 
        • max-parallel-workers 
        • VU configuration match 
    - Selects highest scoring case for accurate vindex assignment

OUTPUT:
    CSV Format: OLTP VU, Vector VU, NOPM, QPS, Run Count, Extension
    
    Files Generated:
        - Default: mixed_workload_results_{db-label}.csv (one per db-label)
        - Combined mode: mixed_workload_results_combined.csv (all results)

    Configuration:
        - Set COMBINE_ALL = True for single combined CSV
        - Set COMBINE_ALL = False for separate CSV per db-label (default)

HANDLES:
    Multiple num-concurrency values per test (creates separate rows)
    Multiple run counts (run_count: 0, 1, 2, ...)
    Multiple test cases with same db-label
    Different vector index types (pgvector, pgvector-bq, pgdiskann)

"""



import os
import re
import json
import csv
from pathlib import Path
from collections import defaultdict

def extract_vindex_to_extension(vindex):
    """Map vindex to extension name."""
    mapping = {
        "hnsw": "pgvector",
        "hnsw_bq": "pgvector-bq",
        "diskann": "pgdiskann"
    }
    return mapping.get(vindex, vindex)

def extract_db_label_from_path(path):
    """Extract db-label from path."""
    parts = Path(path).parts
    
    # Look for the db-label directory (3 levels deep from results/)
    # Pattern: results/pgvector/hnsw/[db-label]/local/...
    for i, part in enumerate(parts):
        if part == 'results' and i + 3 < len(parts):
            return parts[i + 3]
    
    return "unknown"

def extract_test_parameters_from_log(log_path):
    """
    Extract test parameters from log.txt to match against config.json cases.
    Returns dict with parameters found in the log file.
    """
    params = {}
    
    # Check if log.txt exists
    log_file = os.path.join(os.path.dirname(log_path), 'log.txt')
    if not os.path.exists(log_file):
        return params
    
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Extract parameters that might differ between cases
        param_patterns = {
            'm': r'm:\s*(\d+)',
            'ef-construction': r'ef-construction:\s*(\d+)',
            'maintenance-work-mem': r'maintenance-work-mem:\s*(\S+)',
            'max-parallel-workers': r'max-parallel-workers:\s*(\d+)',
            'quantization-type': r'quantization-type:\s*(\S+)',
            'reranking': r'reranking:\s*(\S+)',
        }
        
        for param_name, pattern in param_patterns.items():
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                params[param_name] = match.group(1)
        
        return params
    
    except Exception as e:
        print(f"Warning: Could not read log.txt: {e}")
        return params

def match_case_from_config(config_path, test_params, summaries):
    """
    Match the current test result to the correct case in config.json.
    Uses multiple parameters to find the best match.
    
    Args:
        config_path: Path to config.json
        test_params: Parameters extracted from log.txt
        summaries: Summary data from hdbxtprofile.log (contains VU counts)
    
    Returns:
        The matched case dict, or None if no match found
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        cases = config.get('cases', [])
        
        if len(cases) == 1:
            # Only one case, return it
            return cases[0]
        
        # Try to match based on multiple parameters
        best_match = None
        best_score = 0
        
        for case in cases:
            score = 0
            
            # Match based on index parameters
            if test_params.get('m') == str(case.get('m')):
                score += 3
            if test_params.get('ef-construction') == str(case.get('ef-construction')):
                score += 3
            if test_params.get('maintenance-work-mem') == case.get('maintenance-work-mem'):
                score += 2
            if test_params.get('max-parallel-workers') == str(case.get('max-parallel-workers')):
                score += 2
            if test_params.get('quantization-type') == case.get('quantization-type'):
                score += 2
            if test_params.get('reranking') == case.get('reranking'):
                score += 1
            
            # Try to match based on VU configuration
            # Check if any summary matches the configured mw_oltp_vu and mw_vector_vu
            for summary in summaries:
                if (summary['oltp_vu'] == int(case.get('mw_oltp_vu', 0)) and 
                    summary['vector_vu'] == int(case.get('mw_vector_vu', 0))):
                    score += 5  # Strong match on VU configuration
                    break
            
            if score > best_score:
                best_score = score
                best_match = case
        
        # If we found a reasonable match (score > 0), return it
        if best_score > 0:
            return best_match
        
        # Fallback: return first case
        return cases[0] if cases else None
    
    except Exception as e:
        print(f"Error matching case from config: {e}")
        return None

def extract_all_summaries_from_hdbxtprofile(file_path):
    """
    Extract ALL summary sections from hdbxtprofile.log.
    Returns a list of dictionaries, one for each SUMMARY section.
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Find ALL SUMMARY sections using finditer 
        summary_pattern = re.compile(
            r'>>>>> SUMMARY OF (\d+) ACTIVE VIRTUAL USERS.*?'
            r'TOTAL VECTOR QPS:\s*([\d.]+).*?'
            r'NOPM:\s*(\d+)',
            re.DOTALL
        )
        
        summaries = []
        
        for match in summary_pattern.finditer(content):
            total_vu = int(match.group(1))
            qps = float(match.group(2))
            nopm = int(match.group(3))
            
            # Get the position of this summary in the file
            summary_start = match.start()
            
            # Count how many VIRTUAL USER sections have SEMANTIC_SEARCH before this summary
            vector_vu_pattern = r'>>>>> VIRTUAL USER \d+ :.*?>>>>> PROC: SEMANTIC_SEARCH'
            
            # We need to count only the ones in the CURRENT test section
            # Find the previous summary's position
            previous_summaries = list(summary_pattern.finditer(content[:summary_start]))
            
            if previous_summaries:
                # Get content between previous summary and current summary
                previous_summary_end = previous_summaries[-1].end()
                current_section = content[previous_summary_end:summary_start]
            else:
                # This is the first summary, get content from start
                current_section = content[:summary_start]
            
            # Count SEMANTIC_SEARCH in current section
            vector_vu_matches = re.findall(vector_vu_pattern, current_section, re.DOTALL)
            vector_vu = len(vector_vu_matches)
            
            # OLTP VU = Total VU - Vector VU
            oltp_vu = total_vu - vector_vu
            
            summaries.append({
                'oltp_vu': oltp_vu,
                'vector_vu': vector_vu,
                'nopm': nopm,
                'qps': qps
            })
        
        if not summaries:
            print(f"Warning: No SUMMARY sections found in {file_path}")
            return None
        
        return summaries
    
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def extract_run_count_from_dirname(dirname):
    """Extract run count from directory name."""
    # Pattern: ...-{run_count}-{random_number}
    match = re.search(r'-(\d+)-\d+$', dirname)
    if match:
        return int(match.group(1))
    return 0

def process_results_directory(results_dir):
    """
    Walk through results directory and extract data.
    Returns dict grouped by db-label: {db_label: [rows]}
    """
    data_by_label = defaultdict(list)
    processed_count = 0
    error_count = 0
    
    print(f"\nScanning directory: {results_dir}")
    print("=" * 60)
    
    for root, dirs, files in os.walk(results_dir):
        # Check if this directory contains result files
        if 'hdbxtprofile.log' in files and 'config.json' in files:
            hdbxtprofile_path = os.path.join(root, 'hdbxtprofile.log')
            config_path = os.path.join(root, 'config.json')
            
            # Extract db-label from path
            db_label = extract_db_label_from_path(root)
            
            # Extract ALL summaries (one per num-concurrency)
            summaries = extract_all_summaries_from_hdbxtprofile(hdbxtprofile_path)
            if not summaries:
                error_count += 1
                continue
            
            # Extract test parameters from log.txt
            test_params = extract_test_parameters_from_log(hdbxtprofile_path)
            
            # Match to the correct case in config.json
            matched_case = match_case_from_config(config_path, test_params, summaries)
            
            if matched_case:
                vindex = matched_case.get('vindex', 'unknown')
                extension = extract_vindex_to_extension(vindex)
            else:
                extension = "unknown"
            
            run_count = extract_run_count_from_dirname(os.path.basename(root))
            
            # Create one row for EACH summary (each num-concurrency)
            for idx, summary in enumerate(summaries):
                row = {
                    'OLTP VU': summary['oltp_vu'],
                    'Vector VU': summary['vector_vu'],
                    'NOPM': summary['nopm'],
                    'QPS': summary['qps'],
                    'Run Count': run_count,
                    'Extension': extension,
                    'Concurrency Index': idx  # To track which num-concurrency this is
                }
                
                data_by_label[db_label].append(row)
            
            processed_count += 1
            print(f"Processed: {os.path.basename(root)} → {db_label} ({len(summaries)} summaries) → {extension}")
    
    print("=" * 60)
    print(f"Summary: {processed_count} folders processed, {error_count} errors")
    
    return data_by_label

def write_to_csv(data, output_file):
    """Write data to CSV file."""
    if not data:
        print(f"No data to write for {output_file}")
        return
    
    fieldnames = ['OLTP VU', 'Vector VU', 'NOPM', 'QPS', 'Run Count', 'Extension']
    
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        
        print(f"CSV created: {output_file} ({len(data)} rows)")
    
    except Exception as e:
        print(f"Error writing CSV {output_file}: {e}")

def main():
    results_base_dir = "results"
    
    # Set COMBINE_ALL to True to combine all db-labels into one CSV file
    # Set to False to create separate CSV files for each db-label (DEFAULT)
    COMBINE_ALL = False  # ← Change this to True to combine all results
    
    # Process directories grouped by db-label
    data_by_label = process_results_directory(results_base_dir)
    
    if not data_by_label:
        print("\nNo data found to process!")
        return
    
    print(f"\nFound {len(data_by_label)} db-label(s):")
    for label, rows in data_by_label.items():
        print(f"   • {label} ({len(rows)} results)")
    
    if COMBINE_ALL:
        # Combine all data into one CSV
        print("\nCombining all results into one file...")
        all_data = []
        for db_label, rows in data_by_label.items():
            all_data.extend(rows)
        
        # Sort combined data
        all_data.sort(key=lambda x: (
            x['Extension'], 
            x['OLTP VU'], 
            x['Vector VU'], 
            x['Run Count'],
            x.get('Concurrency Index', 0)
        ))
        
        output_csv = "mixed_workload_results_combined.csv"
        write_to_csv(all_data, output_csv)
    
    else:
        # Create separate CSV for each db-label
        print("\nCreating separate CSV files for each db-label...")
        for db_label, rows in data_by_label.items():
            # Sort data for this db-label
            rows.sort(key=lambda x: (
                x['Extension'], 
                x['OLTP VU'], 
                x['Vector VU'], 
                x['Run Count'],
                x.get('Concurrency Index', 0)
            ))
            
            # Create CSV filename from db-label
            output_csv = f"mixed_workload_results_{db_label}.csv"
            write_to_csv(rows, output_csv)
    
    print("\nProcessing complete!")

if __name__ == "__main__":
    main()