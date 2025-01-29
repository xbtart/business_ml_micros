import pandas as pd
import matplotlib.pyplot as plt
import time
import os

log_file = "./logs/metric_log.csv"
output_image = "./logs/error_distribution.png"

# Проверяем существование файла логов
if not os.path.exists(log_file):
    print(f"Файл {log_file} не найден. Ожидание данных...")
    while not os.path.exists(log_file):
        time.sleep(5)

# Бесконечный цикл для обновления графика
while True:
    try:
        # Загружаем данные из CSV
        data = pd.read_csv(log_file)
        
        # Проверяем, есть ли данные
        if not data.empty:
            # Строим гистограмму абсолютных ошибок
            plt.figure(figsize=(8, 6))
            plt.hist(data["absolute_error"], bins=20, color='skyblue', edgecolor='black')
            plt.title("Распределение абсолютных ошибок")
            plt.xlabel("Абсолютная ошибка")
            plt.ylabel("Частота")
            plt.grid(True)
            
            # Сохраняем график в файл
            plt.savefig(output_image)
            plt.close()
            print(f"Гистограмма сохранена в {output_image}")
        else:
            print("Нет данных для построения графика.")

        # Задержка перед следующим обновлением
        time.sleep(10)
    
    except Exception as e:
        print(f"Ошибка при построении графика: {e}")
        time.sleep(10)
