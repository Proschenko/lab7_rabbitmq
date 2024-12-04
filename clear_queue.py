import pika

# Настройка соединения с RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Удаление очереди
queue_name = 'links_queue'
channel.queue_declare(queue=queue_name, durable=True)  # Объявление очереди, если она существует

# Очистка очереди
channel.queue_purge(queue_name)

print(f"Queue {queue_name} has been purged.")

# Закрытие соединения
connection.close()
