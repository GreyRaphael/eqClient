from queue import Queue
import threading
import time


class App:
    def __init__(self, q: Queue):
        self.thd = threading.Thread(target=self.on_data, args=(q,))

    def on_data(self, q: Queue):
        for i in range(5):
            time.sleep(0.2)
            q.put(i)
            print("produce===>", i)
        time.sleep(10)
        q.put(666)
        print("produce===> 666")

    def wait(self):
        self.thd.join()

    def start(self):
        self.thd.start()


def worker(q: Queue):
    while True:
        time.sleep(0.5)
        record = q.get()
        print("--->consume", record)
        q.task_done()


def func():
    q = Queue(maxsize=32)
    app = App(q)
    app.start()
    threading.Thread(target=worker, args=(q,), daemon=True).start()
    app.wait()
    q.join()
    print("app stop")


if __name__ == "__main__":
    func()
