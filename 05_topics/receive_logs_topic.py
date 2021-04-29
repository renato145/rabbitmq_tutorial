import pika, sys
from pika.exchange_type import ExchangeType

exchange_name = 'topic_logs'

binding_keys = sys.argv[1:]
if not binding_keys:
    sys.stderr.write(f'Usage: {sys.argv[0]} [binding_key]...\n')
    sys.exit(1)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.topic)

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)

def callback(ch, method, properties, body):
    print(f' [x] "{method.routing_key}:{body.decode()}"')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()
