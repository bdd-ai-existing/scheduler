# from threading import Thread

# def batch_processor(items, func, batch_size=10):
#     """
#     Process items in batches using threads.
#     :param items: List of items to process.
#     :param func: Function to apply to each batch.
#     :param batch_size: Size of each batch.
#     """
#     for i in range(0, len(items), batch_size):
#         batch = items[i:i + batch_size]
#         thread = Thread(target=func, args=(batch,))
#         thread.start()
#         thread.join()

import asyncio

async def batch_processor(items, func, batch_size=10):
    """
    Process items in batches asynchronously.
    :param items: List of items to process.
    :param func: Async function to apply to each batch.
    :param batch_size: Size of each batch.
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        await func(batch)  # Await the async function for each batch
