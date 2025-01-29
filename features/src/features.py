import pika
import numpy as np
import json
import time
from datetime import datetime
from sklearn.datasets import load_diabetes

while True:
    try:
        # Загружаем датасет о диабете
        X, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0])

        # Генерируем уникальный идентификатор сообщения
        message_id = datetime.timestamp(datetime.now())

        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Создаём очереди, если их нет
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Формируем сообщения с уникальным идентификатором
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
        }

        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }

        # Публикуем сообщение в очередь y_true
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(message_y_true))
        print(f"[{message_id}] Сообщение с правильным ответом отправлено в очередь")

        # Публикуем сообщение в очередь features
        channel.basic_publish(exchange='',
                              routing_key='features',
                              body=json.dumps(message_features))
        print(f"[{message_id}] Сообщение с вектором признаков отправлено в очередь")

        # Закрываем подключение
        connection.close()

        # Добавляем задержку перед следующей итерацией
        time.sleep(10)

    except Exception as e:
        print(f"Не удалось подключиться к очереди: {e}")
        time.sleep(10)