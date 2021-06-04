import asyncio
import string
import random

import attr
import logging
from devtools import debug

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.example.net'


async def amar():
    await asyncio.sleep(1)


async def publish(queue, n):
    choices = string.ascii_lowercase + string.digits
    for x in range(1, n + 1):
        print('loop started')
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg = PubSubMessage(message_id=x, instance_name=instance_name)
        # await queue.put(msg)
        await amar()
        logging.info(f'Published {x} of {n} messages')
        print('loop ended')

    await queue.put(None)  # publisher is done
    debug(queue)


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()
        if msg is None:  # publisher is done
            break

        # process the msg
        logging.info(f'Consumed {msg}')
        # unhelpful simulation of i/o work
      #  await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    asyncio.run(publish(queue, 5), debug=True)
    print('pbulic finished')

    asyncio.run(consume(queue), debug=True)


if __name__ == '__main__':
    main()
