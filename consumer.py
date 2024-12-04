import pika
import time
import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import logging

# Загружаем переменные окружения
load_dotenv()

# Чтение параметров из .env
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT', '5672')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'links_queue')
TIMEOUT = int(os.getenv('TIMEOUT', 10))

# Настройка логирования: консоль и файл
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Создаем обработчик для записи в файл
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)

# Создаем обработчик для вывода в консоль
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Формат логов
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Добавляем обработчики
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# Загружаем обработанные ссылки из файла, чтобы избежать дублей
def load_processed_links():
    if os.path.exists('processed_links.txt'):
        with open('processed_links.txt', 'r') as f:
            return set(f.read().splitlines())
    return set()


# Сохраняем новые обработанные ссылки
def save_processed_links(processed_links):
    with open('processed_links.txt', 'a') as f:
        for link in processed_links:
            f.write(link + '\n')


# Извлекаем все внутренние ссылки из страницы
def extract_links(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/') or url in href:
                links.add(href if href.startswith('http') else url + href)
        return links
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return set()


# Функция обработки сообщений
def callback(ch, method, properties, body):
    url = body.decode()
    logger.info(f"Processing URL: {url}")

    # Загружаем обработанные ссылки
    processed_links = load_processed_links()

    # Извлекаем новые ссылки
    links = extract_links(url)
    new_links = links - processed_links  # Убираем уже обработанные ссылки

    # Отправляем новые ссылки в очередь
    for link in new_links:
        logger.info(f"Sending new link: {link}")
        ch.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=link,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Сообщения будут долговечными
            ))

    # Сохраняем обработанные ссылки
    save_processed_links(new_links)

    # Подтверждаем получение сообщения
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Подключаемся к RabbitMQ
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials))
channel = connection.channel()

# Объявляем очередь
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Таймаут в секундах
timeout = TIMEOUT
start_time = time.time()

# Начинаем потребление сообщений
logger.info("Waiting for messages...")
while True:
    method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME)

    if method_frame is None:
        # Если очередь пуста, проверяем таймаут
        if time.time() - start_time > timeout:
            logger.info(f"Timeout reached: No messages in queue for {timeout} seconds.")
            break
    else:
        # Если сообщение есть, обрабатываем его
        callback(channel, method_frame, header_frame, body)
        start_time = time.time()  # Сброс таймера после успешной обработки

# Закрытие соединения
connection.close()
