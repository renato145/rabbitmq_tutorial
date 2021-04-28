import sys, pika
from pika.exchange_type import ExchangeType

msg = ' '.join(sys.argv[1:]) or 'info: Hello World!'

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='logs', exchange_type=ExchangeType.fanout)
channel.basic_publish(exchange='logs', routing_key='', body=msg)

print(f'[x] Sent {msg!r}')
connection.close()
