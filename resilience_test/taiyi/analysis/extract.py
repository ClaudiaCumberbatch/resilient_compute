import os
import sqlite3
import toml

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
    config_path = find_path(directory, target='config.toml')
    record = {}
    record['run_id'] = ''
    record['run_dir'] = None
    record['workflow'] = None
    record['failure_type'] = None
    record['failure_rate_set'] = None
    record['makespan'] = None
    record['workflow_finish'] = None
    record['average_task_time'] = None
    record['task_success_rate'] = None

    if db_path:
        get_info_from_db(db_path, record)
    else:
        print("No monitoring.db file found in the directory.")
    
    if config_path:
        read_config(config_path, record)
    else:
        print("No config.toml file found in the directory.")
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
    Extract run_id, run_dir, makespan, workflow_finish, average_task_time, and task_success_rate from database.
    Directly modify record.
    Return nothing.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # run_id, run_dir, and makespan
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
            record['workflow_finish'] = True

        # average task time
        cursor.execute('''
            SELECT AVG((julianday(task_time_returned) - julianday(task_time_invoked)) * 86400) AS average_time_in_seconds
            FROM task
        ''')
        rows = cursor.fetchall()
        record['average_task_time'] = rows[0][0]

        # success rate
        cursor.execute('''
            SELECT COUNT(DISTINCT task_id) AS unique_task_count
            FROM try
        ''')
        unique_task_count = cursor.fetchone()[0]

        cursor.execute('''
            SELECT COUNT(*) AS total_record_count
            FROM try
        ''')
        total_record_count = cursor.fetchone()[0]

        if total_record_count > 0:
            task_success_rate = unique_task_count / total_record_count
        else:
            task_success_rate = 0

        record['task_success_rate'] = task_success_rate

        
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
    

def read_config(config_file, record):
    """
    Extract workflow, failure_type, failure_rate_set from config.toml file.
    Directly modify record.
    Return nothing.
    """
    config = toml.load(config_file)
    
    workflow = config.get('app', {}).get('base')
    failure_type = config.get('app', {}).get('failure_type')
    failure_rate_set = config.get('app', {}).get('failure_rate')

    record['workflow'] = workflow
    record['failure_type'] = failure_type
    record['failure_rate_set'] = failure_rate_set
    

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
                makespan REAL,
                workflow_finish BOOLEAN,
                average_task_time REAL,
                task_success_rate REAL
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
            INSERT OR IGNORE INTO workflow (run_id, run_dir, workflow, failure_type, failure_rate_set, makespan, workflow_finish, average_task_time, task_success_rate)
            VALUES (:run_id, :run_dir, :workflow, :failure_type, :failure_rate_set, :makespan, :workflow_finish, :average_task_time, :task_success_rate)
        ''', filtered_data)
        conn.commit()
    except sqlite3.Error as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    root_dir = '/work/cse-zhousc/resilient_compute/resilience_test/taiyi/debug/runs'
    dirs = get_all_directories(directory=root_dir)

    db_path = 'example.db'
    create_database_and_table_if_not_exists(db_path)
    record_list = []

    for dir in dirs:
        r = get_one_record(dir)
        record_list.append(r)
        
    insert_or_append_data(db_path, record_list)
        

    
