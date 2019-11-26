import asyncio
import random
import string
import unittest
from collections import defaultdict

from kubernetes.client.models.v1_object_meta import V1ObjectMeta
from kubernetes.client.models.v1_pod import V1Pod

from . import fake_controller_source, wait
from .controller import (
    ResourceEventHandlerFuncs,
    deletion_handling_meta_namespace_key_func,
    new_informer,
)


def run(main, *, debug=False):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.set_debug(debug)
        return loop.run_until_complete(main)
    finally:
        try:
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


def _cancel_all_tasks(loop):
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(
        asyncio.gather(*to_cancel, loop=loop, return_exceptions=True)
    )

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def async_test(coro):
    def wrapper(*args, **kwargs):
        return run(coro(*args, **kwargs))

    return wrapper


class TestController(unittest.TestCase):
    @async_test
    async def test_hammer(self):
        source = fake_controller_source.FakeControllerSource()
        output_set_lock = asyncio.Lock()
        output_set = defaultdict(list)

        async def record_func(event_type, obj):
            key = deletion_handling_meta_namespace_key_func(obj)
            async with output_set_lock:
                output_set[key].append(event_type)

        async def add_func(obj):
            await record_func("add", obj)

        async def update_func(old_obj, new_obj):
            await record_func("update", new_obj)

        async def delete_func(obj):
            await record_func("delete", obj)

        h = ResourceEventHandlerFuncs(
            add_func=add_func, update_func=update_func, delete_func=delete_func
        )
        _, controller = new_informer(source, V1Pod(), 0.1, h)
        self.assertFalse(controller.has_synced())

        stop = asyncio.Event()
        asyncio.ensure_future(controller.run(stop))

        await wait.poll(0.1, wait.FOREVER_TEST_TIMEOUT, controller.has_synced)
        self.assertTrue(controller.has_synced())

        async def task():
            current_names = set()
            for _ in range(100):
                if not current_names or not random.randrange(3):
                    name = "".join(
                        random.choice(string.ascii_letters) for _ in range(16)
                    )
                    is_new = True
                else:
                    name = random.choice(list(current_names))
                    is_new = False
                # TODO: fuzz
                pod = V1Pod(metadata=V1ObjectMeta(name=name, namespace="default"))
                if is_new:
                    current_names.add(name)
                    await source.add(pod)
                    continue
                if random.randrange(2):
                    current_names.add(name)
                    await source.modify(pod)
                else:
                    current_names.remove(name)
                    await source.delete(pod)

        await asyncio.gather(*(task() for _ in range(3)))

        await asyncio.sleep(0.1)
        stop.set()
        # TODO: Figure out why this is necessary...
        await asyncio.sleep(0.1)

        await output_set_lock.acquire()


if __name__ == "__main__":
    unittest.main()
