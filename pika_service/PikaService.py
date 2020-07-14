import pika
import json


class PikaService:
    connection = create_connection()
    channel = create_channel()

    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters

    def create_connection(self):
        host = self.connection_parameters['host']
        return pika.BlockingConnection(pika.ConnectionParameters(host=host))

    def create_channel(self):
        return self.connection.channel()

    def create_queue(self, queue):
        return self.channel.queue_declare(queue=queue)

    def create_consumer(self, queue, callback, auto_ack):
        return self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=auto_ack)

    def create_exchange(self, exchange, exchange_type):
        return self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

    def create_json_message(self, message):
        return json.dumps(message)

    def publish_to_route(self, body, exchange, routing_key, is_json=True, print_message=False):
        if is_json:
            body = self.create_json_message(body)
        self.channel.basic_publish(body=body, exchange=exchange, routing_key=routing_key)
        if print_message:
            print(body)

    def consume_by_route(self, callback, exchange, queue_name, routing_key):
        self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)
        self.channel.start_consuming()
