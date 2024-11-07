import os
import re
import sqlite3
import toml
from datetime import datetime, timedelta


def get_all_directories(directory: str) -> list:
    """
    Get all dirs under given dir. 
    Here we set given directory='runs/', so each returned directory includes one workflow.
    """

    entries = os.listdir(directory)
    directories = [os.path.join(directory, entry) for entry in entries if os.path.isdir(os.path.join(directory, entry))]
    return directories


def get_one_record(directory: str) -> dict:
    """
    Given a workflow directory, create a workflow record.
    Extract information from database and toml file respectively.
    """

    db_path = find_path(directory, target='monitoring.db')
    log_path = find_path(directory, target='log.txt')
    record = {}
    record['run_id'] = ''
    record['run_dir'] = None
    record['workflow'] = None
    record['failure_type'] = None
    record['failure_rate_set'] = None
    record['makespan'] = None
    record['workflow_finish'] = None
    record['average_task_time'] = None
    record['task_count'] = 0
    record['task_success_rate'] = None
    record['retry_success_rate'] = None
    record['failure_track'] = None
    record['resilience'] = None
    record['overhead'] = 0
    record['executor_cnt'] = 0
    record['node_cnt'] = 0

    if db_path:
        get_info_from_db(db_path, record)
    else:
        print("No monitoring.db file found in the directory.")
    
    if log_path:
        read_log(log_path, record)
    else:
        print("No log.txt file found in the directory.")
    return record


def find_path(directory: str, target: str) -> str:
    """
    Find monitoring.db or config.toml according to the given target value.
    """
    for root, dirs, files in os.walk(directory):
        if target in files:
            return os.path.join(root, target)
    return None


def get_info_from_db(db_path: str, record: dict):
    """
    Extract run_id, run_dir, makespan, workflow_finish, average_task_time, task_count, task_success_rate, retry_success_rate, and failure_track from database.
    makespan indicates whether the workflow finished by itself,
    while workflow_finish indicates whether the workflow finished successfully.
    failure_track is a test feature for mapreduce (3 maps, 1 reduce). 
    It's a string seperated by one comma, indicating the number of failures in map period and reduce period.
    Directly modify record.
    Return nothing.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # run_id, run_dir, and makespan
        """
        If a workflow finishes by itself, there will be a makespan. 
        Otherwise, it is killed by main.sh based due to time limit.
        This is actually useful for debugging.
        """
        cursor.execute("SELECT run_id, rundir, (julianday(time_completed) - julianday(time_began))* 86400 AS makespan FROM workflow")
        rows = cursor.fetchall()
        if not rows:
            return

        record['run_id'] = rows[0][0]
        record['run_dir'] = rows[0][1]
        record['makespan'] = rows[0][2]

        # workflow_finish
        cursor.execute('''
            SELECT * FROM workflow
            WHERE tasks_failed_count = 0
            AND tasks_completed_count != 0
        ''')
        rows = cursor.fetchall()
        if len(rows) == 0:
            record['workflow_finish'] = False
        else:
            """
            To make sure the workflow finishes successfully,
            the number of tasks_completed_count should be equal to the number of tasks in task table.
            """
            cursor.execute('''
                SELECT COUNT(*) AS task_count
                FROM task
            ''')
            task_count = cursor.fetchone()[0]
            
            if rows[0][-1] == task_count:
                record['workflow_finish'] = True
            else:
                record['workflow_finish'] = False

        # average task time, task count
        cursor.execute('''
            SELECT COUNT(*) AS task_count, AVG((julianday(task_time_returned) - julianday(task_time_invoked)) * 86400) AS average_time_in_seconds
            FROM task
        ''')
        rows = cursor.fetchall()
        record['task_count'] = rows[0][0]
        record['average_task_time'] = rows[0][1]

        # task success rate
        """
        A successful try should have task_try_time_returned not null and task_fail_history null.
        """
        cursor.execute('''
            SELECT COUNT(*) AS success_count
            FROM try
            WHERE task_try_time_returned IS NOT NULL
                AND task_fail_history IS ''
        ''')
        success_task_count = cursor.fetchone()[0]

        cursor.execute('''
            SELECT COUNT(*) AS total_record_count
            FROM try
        ''')
        total_record_count = cursor.fetchone()[0]

        if total_record_count > 0:
            task_success_rate = success_task_count / total_record_count
        else:
            task_success_rate = 0

        record['task_success_rate'] = task_success_rate

        # retry_success_rate
        cursor.execute('''
            SELECT COUNT(*) AS success_retry
            FROM try
            WHERE task_try_time_returned IS NOT NULL
                AND task_fail_history IS ''
                AND try_id != 0
        ''')
        success_retry_count = cursor.fetchone()[0]
        cursor.execute('''
            SELECT COUNT(*) AS total_retry
            FROM try
            WHERE try_id != 0
        ''')
        total_retry_count = cursor.fetchone()[0]
        if total_retry_count > 0:
            retry_success_rate = success_retry_count / total_retry_count
        else:
            retry_success_rate = 0

        record['retry_success_rate'] = retry_success_rate

        # failure_track
        cursor.execute('''
            SELECT COUNT(*) AS failure_count_map
            FROM try
            WHERE try_id = 0
            AND task_fail_history IS NOT ''
            AND (task_id IS '0' OR task_id IS '1' OR task_id IS '2')
        ''')
        failure_count_map = cursor.fetchone()[0]
        cursor.execute('''
            SELECT COUNT(*) AS failure_count_reduce
            FROM try
            WHERE try_id = 0
            AND task_fail_history IS NOT ''
            AND task_id IS '3'
        ''')
        failure_count_reduce = cursor.fetchone()[0]
        record['failure_track'] = f'{failure_count_map},{failure_count_reduce}'
        
        
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def get_overhead(log_file) -> float:
    """
    Calculate the overhead of retry_handler.
    Takes 30s to run.
    """
    def extract_timestamp(line: str):
        retry_config_pattern = re.compile(r'\[(.*?)\] INFO  \(retry_config\)')
        time_format = '%Y-%m-%d %H:%M:%S.%f'
        match = retry_config_pattern.search(line)
        if match:
            timestamp_str = match.group(1)
            return datetime.strptime(timestamp_str, time_format)
        return None
    
    with open(log_file, 'r') as file:
        lines = file.readlines()

    time_deltas = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if 'retry_config' in line:
            # start to look for the last consecutive retry_config
            j = i + 1
            while j < len(lines) and 'retry_config' in lines[j]:
                j += 1

            start_time = extract_timestamp(lines[i])
            end_time = extract_timestamp(lines[j - 1])
            if start_time is not None and end_time is not None:
                time_deltas.append(end_time - start_time)
                # print(f'retry_config 调用时间: {start_time} - {end_time}')
            i = j
        else:
            i += 1

    total_time = sum(time_deltas, timedelta())
    return total_time.total_seconds()


def read_log(log_file, record):
    """
    Extract workflow, failure_type, failure_rate_set, resilience, overhead, executor_cnt, node_cnt from log file.
    Directly modify record.
    Return nothing.
    """

    # executor_cnt
    executor_pattern = r'\n\s+\w+:\s*\n\s+address:'  # Matches lines expected for executors.
    with open(log_file, 'r') as file:
        log_data = file.read()  # Read the entire content of the log file.
    matches = re.findall(executor_pattern, log_data)
    record['executor_cnt'] = len(matches)

    # workflow, failure_type, failure_rate_set, resilience, node_cnt
    with open(log_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if 'base' in line:
                if len(line.split(': ')) < 2:
                    return
                record['workflow'] = line.split(': ')[1].strip().strip("'")
            elif 'failure_rate' in line:
                record['failure_rate_set'] = float(line.split(': ')[1].strip())
            elif 'failure_type' in line:
                record['failure_type'] = line.split(': ')[1].strip().strip("'")
            elif 'retry_handler' in line:
                if 'resilient_retry' in line:
                    record['resilience'] = True
                    record['overhead'] = get_overhead(log_file)
                else:
                    record['resilience'] = False
            elif 'nodes_per_block' in line:
                """
                The number of nodes in total is a bit hard to determine.
                Here we use a simple way, i.e. assume the number of blocks per executor = 1
                then add up the number of nodes in each executor.
                """
                record['node_cnt'] = record['node_cnt'] + int(line.split('=')[1].strip().rstrip(','))
            elif '(parsl.dataflow.dflow) > Parsl version:' in line:
                # indicates the end of config and the beginning of workflow
                return


def create_database_and_table_if_not_exists(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS workflow (
                run_id TEXT PRIMARY KEY,
                run_dir TEXT,
                workflow TEXT,
                failure_type TEXT,
                failure_rate_set REAL,
                resilience BOOLEAN,
                overhead REAL,
                executor_cnt INTEGER,
                node_cnt INTEGER,
                makespan REAL,
                workflow_finish BOOLEAN,
                average_task_time REAL,
                task_count INTEGER,
                task_success_rate REAL,
                retry_success_rate REAL,
                failure_track TEXT
            )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred while creating the table: {e}")
    finally:
        cursor.close()
        conn.close()


def insert_or_append_data(db_path: str, data: list):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    conn.set_trace_callback(print)

    # Filter out records without run_id
    filtered_data = [entry for entry in data if entry.get('run_id')]
    
    try:
        cursor.executemany('''
            INSERT OR IGNORE INTO workflow (
                           run_id, 
                           run_dir, 
                           workflow, 
                           failure_type, 
                           failure_rate_set, 
                           resilience, 
                           overhead,
                           executor_cnt, 
                           node_cnt, 
                           makespan, 
                           workflow_finish, 
                           average_task_time, 
                           task_count,
                           task_success_rate, 
                           retry_success_rate,
                           failure_track)
            VALUES (
                           :run_id, 
                           :run_dir, 
                           :workflow, 
                           :failure_type, 
                           :failure_rate_set, 
                           :resilience, 
                           :overhead,
                           :executor_cnt, 
                           :node_cnt, 
                           :makespan, 
                           :workflow_finish, 
                           :average_task_time, 
                           :task_count,
                           :task_success_rate, 
                           :retry_success_rate,
                           :failure_track)
        ''', filtered_data)
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    root_dir = '/work/cse-zhousc/resilient_compute/resilience_test/taiyi/mapreduce/runs'
    # root_dir = '/data/cse-zhousc/log/same_time'
    dirs = get_all_directories(directory=root_dir)

    # db_path = 'example.db'
    db_path = 'mapreduce_correct.db'
    create_database_and_table_if_not_exists(db_path)
    record_list = []

    for dir in dirs:
        r = get_one_record(dir)
        record_list.append(r)
        
    insert_or_append_data(db_path, record_list)
        

    
