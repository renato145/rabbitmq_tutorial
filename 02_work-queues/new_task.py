import sys, pika

msg = ' '.join(sys.argv[1:]) or "Hello World!"

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=msg,
    properties=pika.BasicProperties(
        delivery_mode=2, # make message persistent
    )
)

print(f'[x] Sent {msg!r}')
connection.close()