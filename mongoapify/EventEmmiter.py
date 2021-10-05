#!/usr/bin/env python3

import threading
import logging
import pika
import json

logger = logging.getLogger(__name__)

# TODO silence pka INFO if root.logger < DEBUG
logging.getLogger("pika").setLevel(logging.ERROR)


class EventEmmiter(object):
    def __init__(self, system_bus_name, broadcast_bus_name, conn_url):
        self.system_bus_name = system_bus_name
        self.broadcast_bus_name = broadcast_bus_name
        self.url = conn_url

    def system_event(self, message: dict, msg_tag="default.type"):
        def nonblock_postpone():
            connection = None
            try:
                params = pika.URLParameters(self.url)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.queue_declare(queue=self.system_bus_name, durable=True)
                message["system_bus_id"] = {
                    "bus_name": self.system_bus_name,
                    "msg_tag": msg_tag,
                }

                channel.basic_publish(
                    exchange="",
                    routing_key=self.system_bus_name,
                    body=json.dumps(message, default=str),
                )
                logger.debug(
                    "System messege %s sent to %s bus" % (msg_tag, self.system_bus_name)
                )
            except Exception as ex:
                logger.critical(
                    "System messege %s not sent to %s - with err: %s"
                    % (msg_tag, self.system_bus_name, ex.__str__())
                )
            finally:
                if connection is not None:
                    connection.close()

        d = threading.Thread(name="rabbit-recconect-publish", target=nonblock_postpone)
        d.setDaemon(True)
        d.start()

    def bcast_event(self, message: dict, routing_key="default.type"):
        def nonblock_postpone():
            connection = None
            try:
                params = pika.URLParameters(self.url)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.exchange_declare(exchange=self.broadcast_bus_name, exchange_type="topic")

                channel.basic_publish(
                    exchange=self.broadcast_bus_name, routing_key=routing_key, body=json.dumps(message, default=str)
                )
                logger.debug("Messege %s sent to %s" % (routing_key, self.broadcast_bus_name))
            except Exception as ex:
                logger.critical(
                    "Messege %s not sent to %s - with err: %s"
                    % (routing_key, self.broadcast_bus_name, ex.__str__())
                )
            finally:
                if connection is not None:
                    connection.close()

        d = threading.Thread(name="rabbit-recconect-publish", target=nonblock_postpone)
        d.setDaemon(True)
        d.start()


if __name__ == "__main__":
    pass
