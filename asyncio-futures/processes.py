"""Multiprocessing example with ProcessPoolExecutor."""
import asyncio
import concurrent.futures
from dataclasses import dataclass
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


async def event_source(delay=1) -> typing.AsyncIterator[SourceEvent]:
    """Asynchronous generator of events."""
    counter = 0
    while True:
        await asyncio.sleep(1)
        counter += 1
        event = SourceEvent(index=counter)
        print("New event", event)
        yield event


async def main():
    loop = asyncio.get_running_loop()
    source = event_source()
    with concurrent.futures.ProcessPoolExecutor(max_workers=PROCESS_WORKERS) as pool:
        async for event in source:
            future_ = loop.run_in_executor(pool, process, event)
            print("Submitted to queue", event)


if __name__ == "__main__":
    asyncio.run(main())
