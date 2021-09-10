#!/usr/bin/env python3

import logging
import pika

logger = logging.getLogger(__name__)


class RabbitEmmiter(object):

    def __init__(self, exchanger, amqp_url):
        self.exc = exchanger
        self.url = amqp_url

    def publish(self, message, routing_key="default.type"):
        #wrap - fire-forget
        connection = None
        try:
            params = pika.URLParameters(self.url)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange=self.exc, exchange_type='topic')

            channel.basic_publish(
                exchange=self.exc, routing_key=routing_key, body=message)
            logger.debug("Messege %s sent to %s" % (self.rk, self.exc))
        except Exception as ex:
            logger.critical("Messege %s not sent to %s - with err." %
                            (self.rk, self.exc, ex.__str__()))
        finally:
            if connection is not None:
                connection.close()


if __name__ == "__main__":
    pass
