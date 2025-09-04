import multiprocessing

# maximum number of threads to be used by parallel code blocks
MAX_WORKERS = max(int((multiprocessing.cpu_count()) // 4), 16)
