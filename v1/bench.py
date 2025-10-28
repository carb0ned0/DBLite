import time
from dblite import Client

c = Client()

def bench():
    start = time.time()
    for i in range(10000):
        c.set(f'k{i}', f'v{i}')
    print(f"Set 10k keys: {time.time() - start}s")

bench()