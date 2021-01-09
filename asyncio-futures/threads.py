import asyncio
import concurrent.futures
import time
import threading

MAX_WORKERS = 4
RAISE_EXCEPTION = True

_shutdown = False


def execute_hello(ind):
    print(f"Thread {threading.current_thread().name}: Starting task: {ind}...")

    time.sleep(1)

    if ind == 2 and RAISE_EXCEPTION:
        print("BOOM!")
        raise Exception("Boom!")

    print(f"Thread {threading.current_thread().name}: Finished task {ind}!")
    return ind


def execute_with_shutdown_check(f, *args, **kwargs):
    if _shutdown:
        print(f"Skipping task as shutdown was requested")
        return None

    return f(*args, **kwargs)


async def main_wait():
    loop = asyncio.get_running_loop()
    tasks = range(20)
    global _shutdown
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [loop.run_in_executor(pool, execute_hello, task) for task in tasks]
        done, pending = await asyncio.wait(
            futures, return_when=asyncio.FIRST_EXCEPTION, timeout=3600
        )
        _shutdown = True

    results = [done_.result() for done_ in done]

    if len(pending) > 0:
        raise Exception(f"{len(pending)} tasks did not finish")

    print(f"Finished processing, got results: {results}")


async def main_gather(tasks=20):
    loop = asyncio.get_running_loop()
    global _shutdown
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            loop.run_in_executor(pool, execute_with_shutdown_check, execute_hello, task)
            for task in range(tasks)
        ]
        try:
            results = await asyncio.gather(*futures, return_exceptions=False)
        except Exception as ex:
            print("Caught exception", ex)
            # _shutdown = True
            raise
    print(f"Finished processing, got results: {results}")


# asyncio.run(main_wait())
if __name__ == "__main__":
    asyncio.run(main_gather(tasks=10))
