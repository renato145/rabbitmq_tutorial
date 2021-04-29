import sys, pika
from pika.exchange_type import ExchangeType

exchange_name = 'topic_logs'

routing_key = sys.argv[1] if len(sys.argv) > 1 else 'anonymous.info'
msg = ' '.join(sys.argv[2:]) or 'Hello World!'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.topic)
channel.basic_publish(exchange=exchange_name, routing_key=routing_key, body=msg)

print(f'[x] Sent "{routing_key}:{msg}"')
connection.close()
