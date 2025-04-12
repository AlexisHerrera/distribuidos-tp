import pika

class RabbitMQBroker():
    def __init__(self, host, send_queue, recv_queue):
        self.__connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.__channel = self.__connection.channel()
        self.__send_queue = send_queue

        self.__channel.queue_declare(queue=send_queue)

        self.__channel.basic_consume(queue=recv_queue, on_message_callback=self.__callback, auto_ack=True)

        self.__channel.start_consuming()

    def put(self, body: bytes):
        try:
            self.__channel.basic_publish(exchange='', routing_key=self.__send_queue, body=body)
        except pika.UnroutableError:
            pass
        except pika.NackError:
            pass

    def __callback(self, ch, _method, _properties, body):
        pass


    def stop(self):
        self.__connection.close()