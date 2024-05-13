from parsl.dataflow.taskrecord import TaskRecord

def retry_different_executor(e: Exception,
                             taskrecord: TaskRecord) -> float:
    import random
    dfk = taskrecord['dfk']
    choices = {k: v for k, v in dfk.executors.items() if k != '_parsl_internal' and k!= taskrecord['executor']}
    new_exe = random.choice(list(choices.keys()))
    taskrecord['executor'] = new_exe
    return 1