from src.messaging.publisher import Publisher
from src.messaging.consumer import Consumer


class Connection():
    def __init__(self, broker,  publisher: Publisher, consumer: Consumer):
        self.__broker = broker
        self.__publisher = publisher
        self.__consumer = consumer

    def send(self, message):
        # encode message and send
        self.__publisher.put(self.__broker, message)

    def recv(self, callback):
        def __callback(ch, method, _properties, body):
            print("inside_internal_callback, decoding message")
            try:
                message = body.decode('UTF-8')
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("sent ack")
            except Exception:
                ch.basic_nack(delivery_tag=method.delivery_tag)
                print("sent nack")


        self.__consumer.consume(self.__broker, __callback)

    def close(self):
        self.__broker.close()
