import os
import re
import json
import argparse

parser = argparse.ArgumentParser(description="Combine benchmark results")
parser.add_argument("--benchmark", type=str, choices=["HammerDB", "VectorDBBench"], help="Path to the directory where results are stored")
parser.add_argument("--results-dir-path", type=str, help="Path to the directory where results are stored")
args = parser.parse_args()


def extract_from_hdbxtprofile(file_path):
    with open(file_path, "r") as file:
        content = file.read()

    summary_section = re.search(r'>>>>> SUMMARY OF \d+ ACTIVE VIRTUAL USERS.*?TOTAL VECTOR QPS:.*?TPM:.*?\n', content, re.DOTALL)

    if summary_section:
        summary = summary_section.group()
        qps = re.search(r'TOTAL VECTOR QPS: ([\d.]+)', summary)
        nopm = re.search(r'NOPM: (\d+)', summary)
        tpm = re.search(r'TPM: (\d+)', summary)
        
        p99_latency = re.search(r'P99: ([\d.]+)ms', content)
        
        return {
            "Total Vector QPS": float(qps.group(1)) if qps else None,
            "NOPM": int(nopm.group(1)) if nopm else None,
            "TPM": int(tpm.group(1)) if tpm else None,
            "P99 Latency": float(p99_latency.group(1)) if p99_latency else None
        }
    return {}

def extract_from_log_txt(file_path):
    result = {}
    with open(file_path, "r") as file:
        for line in file:
            if line.startswith("Running command:"):
                break
            if ":" in line:
                key, value = line.split(":", 1)
                result[key.strip()] = value.strip()
    return result

def extract_from_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)

def main():
    for root, _, file_names in os.walk(args.results_dir_path):
        for file_name in file_names:
            print(file_name)
            if file_name == "log.txt":
                bm_run_logs = extract_from_log_txt(os.path.join(root, file_name))
            elif args.benchmark == "HammerDB" and file_name == "hdbxtprofile.log":
                hdbxtprofile = extract_from_hdbxtprofile(os.path.join(root, file_name))
            elif file_name.endswith(".json"):
                vectordb_results = extract_from_json(os.path.join(root, file_name))
    combined_data = bm_run_logs
    combined_data["vectordb"] = vectordb_results["results"]
    if args.benchmark == "HammerDB":
        combined_data["HammerDB"] = hdbxtprofile
    
    with open("combined_results.json", "w") as outfile:
        json.dump(combined_data, outfile, indent=4)

if __name__ == "__main__":
    main()