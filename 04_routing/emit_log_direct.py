import sys, pika
from pika.exchange_type import ExchangeType

exchange_name = 'direct_logs'

severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
msg = ' '.join(sys.argv[2:]) or 'Hello World!'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=ExchangeType.direct)
channel.basic_publish(exchange=exchange_name, routing_key=severity, body=msg)

print(f'[x] Sent "{severity}:{msg}"')
connection.close()
