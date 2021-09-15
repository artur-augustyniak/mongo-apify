#!/usr/bin/env python3

import threading
import logging
import pika

logger = logging.getLogger(__name__)

# print("#" * 80)
# print(logging.root.level)
# print(logging.INFO)
# if logging.root.level <= logging.INFO:
logging.getLogger('pika').setLevel(logging.ERROR)


class RabbitEmmiter(object):

    def __init__(self, exchanger, amqp_url):
        self.exc = exchanger
        self.url = amqp_url

    def publish(self, message, routing_key="default.type"):
        def nonblock_postpone():
            connection = None
            try:
                params = pika.URLParameters(self.url)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.exchange_declare(
                    exchange=self.exc, exchange_type='topic')

                channel.basic_publish(
                    exchange=self.exc, routing_key=routing_key, body=message)
                logger.debug("Messege %s sent to %s" % (routing_key, self.exc))
            except Exception as ex:
                logger.critical("Messege %s not sent to %s - with err: %s" %
                                (routing_key, self.exc, ex.__str__()))
            finally:
                if connection is not None:
                    connection.close()
        d = threading.Thread(name='rabbit-recconect-publish',
                             target=nonblock_postpone)
        d.setDaemon(True)
        d.start()


if __name__ == "__main__":
    pass
