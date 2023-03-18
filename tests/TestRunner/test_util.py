import random
import string
import time

alphanum_letters = string.ascii_letters + string.digits

def random_alphanum_string(length):
    return "".join(random.choice(alphanum_letters) for _ in range(length))

# attach a post-run trace checker to cluster that runs for events between the time of scope entry and exit
class ScopedTraceChecker:
    def __init__(self, cluster, checker_func, filename_substr: str = ""):
        self.cluster = cluster
        self.checker_func = checker_func
        self.filename_substr = filename_substr
        self.begin = None

    def __enter__(self):
        self.begin = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cluster.add_trace_check_from_to(self.checker_func, self.begin, time.time(), self.filename_substr)
