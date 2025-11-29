import time

DEFAULT_INTERVAL_SECONDS = 120


def run_scheduler(dispatch_fn):
    while True:
        dispatch_fn()
        time.sleep(DEFAULT_INTERVAL_SECONDS)
