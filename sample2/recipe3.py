#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Illustrate concurrency with multiple publisher.

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-1/mayhem_3.py

Follow along: https://roguelynn.com/words/asyncio-true-concurrency/
"""

import asyncio
import logging
import random
import string
import uuid
from asyncio import AbstractEventLoop
import attr

# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ("foo %s" % "bar") is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


def bar():
    print('looped')


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


async def publish(queue, publisher_id):
    """Simulates an external publisher of messages.

    Args:
        queue (asyncio.Queue): Queue to publish messages to.
        publisher_id (int): ID of particular publisher.
    """
    choices = string.ascii_lowercase + string.digits

    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        asyncio.create_task(queue.put(msg))
        logging.debug(f"[{publisher_id}] Published message {msg}")
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 0
    #loop.call_soon(bar)
    loop.set_debug(True)

    coros = [publish(queue, i) for i in range(1, 4)]

    try:
        temp1 = []
        for coro in coros:
            t5 = loop.create_task(coro)
            t5.add_done_callback(bar)
            temp1.append(t5)

        loop.run_forever()
    except KeyboardInterrupt:
        logging.debug("Process interrupted")
    finally:
        loop.close()
        logging.info("Successfully shutdown the Mayhem service.")


if __name__ == "__main__":
    main()
