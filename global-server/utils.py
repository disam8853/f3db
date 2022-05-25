import time

def current_time():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    return current_time
    