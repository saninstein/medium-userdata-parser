import time
from workers.medium_worker import MediumDataWorker


workers = [
    MediumDataWorker(),
]

for p in workers:
    p.run()

while True:
    for p in workers:
        p.ping()

    time.sleep(10)
