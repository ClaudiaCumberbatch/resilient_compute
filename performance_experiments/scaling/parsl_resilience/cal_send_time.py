import os
import pprint
import sqlite3

# 递归遍历runinfo文件夹下所有文件，找到resource_monitor.log文件
def find_resource_monitor_log(directory):
    """Recursively search for 'resource_monitor.log' files in the given directory."""
    logs = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file == 'resource_monitor.log':
                logs.append(os.path.join(root, file))
    return logs

# 读取resource_monitor.log文件，提取其中的send_time
def extract_send_time(log):
    send_time = []
    with open(log, 'r') as f:
        for line in f:
            if 'Sent message in' in line:
                # print(line.split()[-2])
                send_time.append(float(line.split()[-2]))
    return send_time

def extract_run_id(log):
    run_id = ''
    with open(log, 'r') as f:
        for line in f:
            if 'Sending message type MessageType.WORKFLOW_INFO content' in line:
                # print(line.split())
                for i in range(len(line.split())):
                    if line.split()[i] == "'run_id':":
                        # print(line.split()[i+1])
                        run_id = line.split()[i+1]
                        if run_id[0] == "'":
                            run_id = run_id[1:-2]
                        return run_id

def find_logs(base_directory):
    results = {}
    run_directories = [d for d in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, d))]

    for run_dir in run_directories:
        parsl_log_path = os.path.join(base_directory, run_dir, 'parsl.log')
        if os.path.exists(parsl_log_path):
            results[parsl_log_path] = []
            for root, dirs, files in os.walk(os.path.join(base_directory, run_dir)):
                for file in files:
                    if file == 'resource_monitor.log':
                        results[parsl_log_path].append(os.path.join(root, file))
    return results


if __name__ == '__main__':
    table_name = "time_cnt"
    print(f"table name: {table_name}")
    db = sqlite3.connect('data.db')
    db.execute(f"""create table if not exists "{table_name}"(
        run_id text,
        average_time float,
        max_time float,
        min_time float
    )""")

    base_directory = '/home/cc/resilient_compute/performance_experiments/scaling/parsl_resilience/runinfo'
    logs_mapping = find_logs(base_directory)
    for parsl_log, resource_logs in logs_mapping.items():
        # print(f'Parsl log: {parsl_log}')
        run_id = extract_run_id(parsl_log)
        # print(f'  - Run ID: {run_id}')
        for resource_log in resource_logs:
            # print(f'  - Resource Monitor log: {resource_log}')
            send_time = extract_send_time(resource_log)
            # print(f'  - Send time: {send_time}')
            # print(f'  - Average send time: {sum(send_time) / len(send_time)}')
            # print(f'  - Max send time: {max(send_time)}')
            # print(f'  - Min send time: {min(send_time)}')
            db.execute(f"""insert into "{table_name}" values (
                "{run_id}",
                {sum(send_time) / len(send_time)},
                {max(send_time)},
                {min(send_time)}
            )""")
    db.commit()
    db.close()