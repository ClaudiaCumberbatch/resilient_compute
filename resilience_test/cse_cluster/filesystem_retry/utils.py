from diaspora_event_sdk import KafkaConsumer
import json
from kafka import TopicPartition
from logging import Logger
import os
import subprocess
from typing import Tuple
import time
import sqlite3
from pathlib import Path
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
from pandas import DataFrame

from parsl.dataflow.taskrecord import TaskRecord
from parsl.serialize import deserialize
from parsl.monitoring.message_type import MessageType


def get_messages_from_db(
        taskrecord: TaskRecord,
        query: str
    ) -> DataFrame:

    def read_all_data_from_table(
            db_path: str, 
            query: str
        ) -> DataFrame:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(query, conn)

        return df
    
    dir_path = taskrecord['dfk'].run_dir
    parent_dir = Path(dir_path).parent
    db_path = ""
    for path in parent_dir.rglob('monitoring.db'):
        db_path = path

    df = read_all_data_from_table(db_path, query)
    return df

def get_messages_from_files(
        taskrecord: TaskRecord,
        topic
    ) -> list:
    """
    here topic is MessageType
    """

    def find_monitor_fs_radio_dirs(root_dir: str) -> list:
        found_dirs = []
        for root, dirs, files in os.walk(root_dir):
            for dir_name in dirs:
                if dir_name == "monitor-fs-radio":
                    dir_path = os.path.join(root, dir_name)
                    found_dirs.append(os.path.abspath(dir_path))
        
        return found_dirs
    
    def path2obj(path):
        with open(path, "rb") as f:
            content = f.read() 
            try:
                obj = deserialize(content)
                return obj
            except Exception as e:
                return None
    
    def get_msgs(directory: str, topic, msg_list) -> list:
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                path = os.path.abspath(file_path)
                if os.path.exists(path):
                    obj = path2obj(path)
                    if obj and obj[0][0] == topic:
                        msg_list.append(obj[0][1])
    
    root_dir = taskrecord['dfk'].run_dir
    monitor_fs_radio_dirs = find_monitor_fs_radio_dirs(root_dir)
    msg_list = []
    for dir in monitor_fs_radio_dirs:
        get_msgs(dir, topic, msg_list)
    return msg_list
    

def update_denylist(
        taskrecord: TaskRecord,
        logger: Logger
    ):
    """
    Update denylist in DataFlowKernel according to try table in database,
    then sort denylist according to success rate.
    """
    query = f"""
    SELECT 
        task_executor,
        COUNT(CASE WHEN task_fail_history = '' THEN 1 END) * 1.0 / COUNT(*) AS success_rate
    FROM 
        try
    WHERE 
        task_try_time_returned IS NOT NULL
    GROUP BY 
        task_executor;
    """
    logger.info(f"current task id is {taskrecord['id']}, try_id is {taskrecord['try_id']}")
    success_df = get_messages_from_db(
        taskrecord=taskrecord,
        query=query
    )
    logger.info(f"success_df is{success_df}")
    denylist = taskrecord['dfk'].denylist
    for index, row in success_df.iterrows():
        denylist[row['task_executor']] = row['success_rate']
    logger.info(f"denylist is {denylist}")
    sorted_denylist = dict(sorted(denylist.items(), key=lambda item: item[1], reverse=True))
    logger.info(f"sorted denylist is {sorted_denylist}")
    taskrecord['dfk'].denylist = sorted_denylist


def time_str_to_seconds(time_str) -> int:
    """
    For Walltime comparison.
    """
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s


def ping_test(hostname: str) -> bool:
    """
    Check whether the node is alive.
    """
    if not hostname:
        return False
    
    for _ in range(3):
        result = subprocess.run(['ping', '-c', '1', hostname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            return True
        
    return False

def get_task_hostname(taskrecord: TaskRecord) -> str:
    """
    Poll try table, get hostname according to current task_id and try_id.
    Using polling because database updating needs time and the hostname could also be None due to the loss of worker.
    """

    for _ in range(10):
        message_df = get_messages_from_db(
            taskrecord=taskrecord,
            query=f"SELECT hostname FROM try WHERE try_id IS {taskrecord['try_id']} AND task_id IS {taskrecord['id']}"
        )
        if len(message_df) > 0 and message_df.iloc[0]['hostname'] is not None:
            return message_df.iloc[0]['hostname']
        else:
            time.sleep(1)

        return None
    