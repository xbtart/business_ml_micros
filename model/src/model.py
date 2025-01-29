import pika
import pickle
import numpy as np
import json
 
# Читаем файл с сериализованной моделью
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)
 
try:
    # Создаём подключение по адресу rabbitmq:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
 
    # Объявляем очередь features
    channel.queue_declare(queue='features')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
 
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        message = json.loads(body)        
        message_id = message["id"]
        features = message["body"]

        print(f"Получен вектор признаков {features}")
        pred = regressor.predict(np.array(features).reshape(1, -1))

        # Формируем сообщение с тем же ID
        prediction_message = {"id": message_id, "body": float(pred[0])}

        channel.basic_publish(
            exchange="", routing_key="y_pred", body=json.dumps(
                prediction_message)
        )
        print(f"Предсказание {pred[0]} отправлено в очередь y_pred")
 
    # Извлекаем сообщение из очереди features
    # on_message_callback показывает, какую функцию вызвать при получении сообщения
    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
 
    # Запускаем режим ожидания прихода сообщений
    channel.start_consuming()
except Exception as ex:
    print(f"Не удалось подключиться к очереди(model.py): {ex}")
