import argparse
import asyncio
import dataclasses
import httpx
import logging
import multiprocessing
import sys
import time
from typing import Any, Dict, Generator, List

# Configure logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("DataPoster")


@dataclasses.dataclass
class RequestStatus:
    time_spent: float
    bytes_transferred: int
    response_code: int


class DataPoster(multiprocessing.Process):
    """Class to post data asynchronously using multiprocessing."""

    def __init__(
        self,
        task_queue: multiprocessing.JoinableQueue,
        result_queue: multiprocessing.Queue,
        base_url: str,
        output_dir: str,
        concurrency: int,
        worker_id: int,
    ):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.base_url = base_url
        self.output_dir = output_dir
        self.concurrency = concurrency
        self.name = f"DataPoster-{worker_id}"

    async def post_data(
        self,
        data: Dict[str, Any],
        session: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> RequestStatus:
        """Make an HTTP POST request to send data."""
        async with semaphore:  # Semaphore limits the number of concurrent posts
            url = self.base_url
            start_time = time.perf_counter()
            resp = await session.post(url, json=data)
            time_spent = time.perf_counter() - start_time
            bytes_transferred = len(resp.content)
            logger.debug(f"Posted data, received response [{resp.status_code}]")
            return RequestStatus(time_spent, bytes_transferred, resp.status_code)

    async def process_data(
        self,
        data: Dict[str, Any],
        session: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
    ) -> RequestStatus:
        """Handle the posting of data."""
        logger.debug(f"{self.name} processing data")
        request_status = await self.post_data(
            data=data, session=session, semaphore=semaphore
        )
        if request_status.response_code != 200:
            logger.error(
                f"{self.name}: POST failed, status={request_status.response_code}"
            )
        return request_status

    async def asyncio_sessions(self) -> None:
        semaphore = asyncio.Semaphore(self.concurrency)
        async with httpx.AsyncClient(http2=True) as session:
            tasks = []
            while True:
                data = self.task_queue.get()
                if data is None:  # Shutdown signal
                    self.task_queue.task_done()
                    break
                task = asyncio.create_task(
                    self.process_data(data=data, session=session, semaphore=semaphore)
                )
                task.add_done_callback(lambda t: self.task_queue.task_done())
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions and send results to the result queue
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error processing task: {result}")
            else:
                self.result_queue.put(result)

    def run(self) -> None:
        asyncio.run(self.asyncio_sessions())


def generate_data(count: int) -> Generator[Dict[str, float], None, None]:
    """Generate dummy data items."""
    for i in range(count):
        yield {"id": i, "price": i * 10.5}


def main() -> None:
    args = parse_clargs()

    # Create queues for tasks and results
    task_queue = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    # Create and start data poster processes
    posters = [
        DataPoster(
            task_queue,
            result_queue,
            "https://nghttp2.org/httpbin/post",
            args.outdir,
            args.concurrency,
            worker_id=i,
        )
        for i in range(args.parallel)
    ]
    for poster in posters:
        poster.start()

    # Enqueue data items to be posted
    for data_item in generate_data(args.count):
        task_queue.put(data_item)

    # Signal shutdown
    for _ in range(args.parallel):
        task_queue.put(None)

    # Wait for all tasks to be done
    task_queue.join()

    # Collect results
    results: List[RequestStatus] = []
    while not result_queue.empty():
        results.append(result_queue.get())
    logger.info(f"Collected {len(results)} results")
    # Log throughput and success rate
    total_time = sum(r.time_spent for r in results)
    total_bytes = sum(r.bytes_transferred for r in results)
    total_requests = len(results)
    throughput = total_bytes / total_time
    success_rate = sum(r.response_code == 200 for r in results) / total_requests
    logger.info(
        f"Total time: {total_time:.2f}s, Total bytes: {total_bytes}, "
        f"Throughput: {throughput:.2f} B/s, Success rate: {success_rate:.2%}"
    )


def parse_clargs() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Asynchronous Data Posting using Multiprocessing and HTTP2"
    )
    parser.add_argument(
        "-c", "--count", type=int, default=101, help="Number of data items to post"
    )
    parser.add_argument(
        "-p", "--parallel", type=int, default=4, help="Number of parallel processes"
    )
    parser.add_argument(
        "-o", "--outdir", default="posteddata", help="Output directory for results"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=25,
        help="Maximum number of concurrent requests",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
