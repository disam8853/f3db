import time

def current_time():
    t = time.localtime()
    return time.strftime("%H:%M:%S", t)
    


def current_date():
    t = time.localtime()
    return time.strftime("%Y-%m-%d", t)
    