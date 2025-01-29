import pika
import json
import os
import pandas as pd

try:
    # Определяем путь к файлу логов
    log_file = "./logs/metric_log.csv"

    # Если файл не существует, создаем его и записываем заголовки
    if not os.path.exists(log_file):
        with open(log_file, "w") as f:
            f.write("id,y_true,y_pred,absolute_error\n")

    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    # Создаем очереди, если их нет
    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_pred')

    # Используем словарь для хранения приходящих данных
    metrics_data = {}

    # Функция обработки сообщений из y_true
    def callback_y_true(ch, method, properties, body):
        message = json.loads(body)
        message_id = message["id"]
        y_true = message["body"]

        # Сохраняем y_true по идентификатору
        if message_id not in metrics_data:
            metrics_data[message_id] = {"y_true": y_true, "y_pred": None}
        else:
            metrics_data[message_id]["y_true"] = y_true

        # Проверяем, можно ли рассчитать ошибку
        log_metrics(message_id)


    # Функция обработки сообщений из y_pred
    def callback_y_pred(ch, method, properties, body):
        message = json.loads(body)
        message_id = message["id"]
        y_pred = message["body"]

        # Сохраняем y_pred по идентификатору
        if message_id not in metrics_data:
            metrics_data[message_id] = {"y_true": None, "y_pred": y_pred}
        else:
            metrics_data[message_id]["y_pred"] = y_pred

        # Проверяем, можно ли рассчитать ошибку
        log_metrics(message_id)

    # Функция логирования метрик
    def log_metrics(message_id):
        if message_id in metrics_data:
            data = metrics_data[message_id]
            if data["y_true"] is not None and data["y_pred"] is not None:
                absolute_error = abs(data["y_true"] - data["y_pred"])
                new_entry = pd.DataFrame([{
                    "id": message_id,
                    "y_true": data["y_true"],
                    "y_pred": data["y_pred"],
                    "absolute_error": absolute_error
                }])

                # Логируем в CSV
                new_entry.to_csv(log_file, mode='a', header=False, index=False)
                print(f"[{message_id}] Лог записан: y_true={data['y_true']}, y_pred={data['y_pred']}, error={absolute_error}")

                # Удаляем запись, так как она уже обработана
                del metrics_data[message_id]
            
            

    # Подписка на очереди
    channel.basic_consume(queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
    channel.basic_consume(queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)

    print("Ожидание сообщений... (нажмите CTRL+C для выхода)")
    channel.start_consuming()
    
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except Exception as ex:
    print(f"Не удалось подключиться к очереди(metric.py): {ex}")
