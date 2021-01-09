"""Multiprocessing example with queue.
"""
import asyncio
import concurrent.futures
from dataclasses import dataclass
import multiprocessing as mp
import time
import typing

PROCESS_WORKERS = 4


@dataclass(frozen=True)
class SourceEvent:
    index: int


@dataclass(frozen=True)
class EventProcessingResult:
    event: SourceEvent
    result: int


def process(event: SourceEvent):
    """Example of CPU-bound operation blocking the event loop."""
    print("Starting processing", event)
    time.sleep(5)
    result = event.index * event.index
    print("Finished processing", event, result)
    return result


async def event_source(delay=1, finish_after=10) -> typing.AsyncIterator[SourceEvent]:
    """Asynchronous generator of events."""
    counter = 0
    while counter < finish_after:
        await asyncio.sleep(delay)
        counter += 1
        event = SourceEvent(index=counter)
        print("New event", event)
        yield event


async def source_to_queue(source, queue):
    async for event in source:
        queue.put(event)
        print("Submitted to queue", event)


def worker(queue, process_event):
    while True:
        event = queue.get()
        if event is None:
            print("Got None, exiting queue")
            return
        result = process_event(event)


async def main():
    loop = asyncio.get_running_loop()
    source = event_source()

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=PROCESS_WORKERS
    ) as pool, mp.Manager() as manager:
        q = manager.Queue()  # type: ignore
        queue_push_task = loop.create_task(source_to_queue(source, queue=q))
        worker_fs = [
            loop.run_in_executor(pool, worker, q, process)
            for _ in range(PROCESS_WORKERS)
        ]
        await queue_push_task
        print("Source finished, pushing None to queue...")
        for _ in range(PROCESS_WORKERS):
            q.put(None)
        await asyncio.gather(*worker_fs)


if __name__ == "__main__":
    asyncio.run(main())
