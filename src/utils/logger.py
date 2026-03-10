import functools
from datetime import datetime


def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"RUNNING: {func.__name__} {start_time.strftime('%H:%M:%S.%f')[:-3]}")
        result = func(*args, **kwargs)
        end_time = datetime.now()
        print(
            f"EXECUTED: {func.__name__} {end_time.strftime('%H:%M:%S.%f')[:-3]}\n"
        )
        return result
    return wrapper