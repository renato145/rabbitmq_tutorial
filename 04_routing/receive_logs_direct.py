import pika, sys
from pika.exchange_type import ExchangeType

exchange_name = 'direct_logs'

severities = sys.argv[1:]
if not severities:
    sys.stderr.write(f'Usage: {sys.argv[0]} [info] [warning] [error]\n')
    sys.exit(1)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.direct)

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

for severity in severities:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=severity)

def callback(ch, method, properties, body):
    print(f' [x] "{method.routing_key}:{body.decode()}"')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()
