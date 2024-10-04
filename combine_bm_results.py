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
    summaries = re.findall(
        r'>>>>> SUMMARY OF ([\d.]+) ACTIVE VIRTUAL USERS.*?AVG:\s*([\d.]+)ms.*?P99:\s*([\d.]+).*?TOTAL VECTOR QPS:\s*([\d.]+).*?NOPM:\s*(\d+).*?TPM:\s*(\d+)',
        content, re.DOTALL | re.IGNORECASE
    )
    results = {}

    for summary in summaries:
        user_count = int(summary[0])
        avg_latency = float(summary[1])
        p99_latency = float(summary[2])
        total_vector_qps = float(summary[3])
        tpm = int(summary[4])
        nopm = int(summary[5])

        results[user_count] = {
            "Total Vector QPS": total_vector_qps,
            "TPM": tpm,
            "NOPM": nopm,
            "avg_latency": avg_latency,
            "P99 Latency": p99_latency
        }

    return results

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
        combined_data = {}
        for file_name in file_names:
            if file_name == "log.txt":
                combined_data["config"] = extract_from_log_txt(os.path.join(root, file_name))
            elif args.benchmark == "HammerDB" and file_name == "hdbxtprofile.log":
                combined_data["HammerDB"] = extract_from_hdbxtprofile(os.path.join(root, file_name))
            elif file_name.startswith("result") and file_name.endswith(".json"):
                combined_data["vectordb"] = extract_from_json(os.path.join(root, file_name))['results']

        if len(file_names) > 0:
            result_file_name = root.split("/")[-1] + "-results.json"
            with open(os.path.join(root, result_file_name), "w") as result_file:
                json.dump(combined_data, result_file, indent=4)
                print(f"File saved in {root}")

    print("Results combined successfully")

if __name__ == "__main__":
    main()
