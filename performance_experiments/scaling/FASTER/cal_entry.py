from diaspora_event_sdk import KafkaConsumer
from diaspora_event_sdk import Client as GlobusClient
import pprint
import time
import sqlite3
import signal

# TODO
# 从runinfo/monitoring.db中读resource记录条数和run_id
# 计算diaspora对应run_id的记录条数，把两个数据插入data.db

def get_runid():
    conn = sqlite3.connect('monitoring.db')
    cursor = conn.cursor()
    cursor.execute("SELECT run_id FROM workflow")
    run_ids = [id[0] for id in cursor.fetchall()]
    print(run_ids)
    return run_ids

def cal_record(run_ids):
    for message in consumer:
        if message.key is None:
            continue
        message_key_str = message.key.decode('utf-8')
        # print(f"message key: {message_key_str}")
        # break
        if message_key_str in run_ids:
            record_per_workflow[message_key_str] = record_per_workflow.get(message_key_str, 0) + 1
            # print(record_per_workflow)

# 定义一个处理函数，当接收到超时信号时，它会抛出一个异常
def handler(signum, frame):
    raise Exception("Timeout!")

if __name__ == '__main__':
    run_ids = get_runid()

    # 设置超时信号
    signal.signal(signal.SIGALRM, handler)

    try:
        # 设置10秒的超时
        signal.alarm(30)
            
        c = GlobusClient()
        topic = "radio-test"
        consumer = KafkaConsumer(topic, auto_offset_reset='earliest')
        # consumer = KafkaConsumer(topic)
        record_per_workflow = {}
        cal_record(run_ids)

    except Exception as e:
        print(e)

    finally:
        # 取消超时
        signal.alarm(0)
        pprint.pprint(record_per_workflow)
        
        # 获取monitoring.db中resource表格中不同run_id的记录条数
        conn = sqlite3.connect('monitoring.db')
        cursor = conn.cursor()
        cursor.execute("SELECT run_id, count(run_id) FROM resource GROUP BY run_id")
        resource_record = cursor.fetchall()
        resource_record = dict(resource_record)
        pprint.pprint(resource_record)
        # 将两个数据插入data.db
        conn = sqlite3.connect('data.db')
        
        # table_name = f'entry-cnt-{time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())}'
        table_name = "entry_cnt"
        print(f"table name: {table_name}")
        db = sqlite3.connect('data.db')
        # 如果之前有这个表格，先删除
        db.execute(f"drop table if exists {table_name}")
        db.execute(f"""create table if not exists "{table_name}"(
            run_id text,
            radio_type text,
            app_name text,
            record_num int)"""
        )
        app_name = 'noop'
        cursor = conn.cursor()
        for run_id, record_num in record_per_workflow.items():
            cursor.execute(f"""
                           INSERT INTO "{table_name}" (run_id, radio_type, app_name, record_num)
                           VALUES (?, ?, ?, ?)""", (run_id, 'diaspora', app_name, record_num))
        for run_id, record_num in resource_record.items():
            cursor.execute(f"""
                           INSERT INTO "{table_name}" (run_id, radio_type, app_name, record_num)
                           VALUES (?, ?, ?, ?)""", (run_id, 'htex', app_name, record_num))
        conn.commit()
        conn.close()
        
