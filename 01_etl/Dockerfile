# Укажите необходимую версию python
FROM python:3.9

# Папка, в которой будут размещаться файлы проекта внутри контейнера
WORKDIR /opt/app

# Копируем в контейнер файлы, которые редко меняются
COPY requirements.txt requirements.txt

# Установка зависимостей
RUN pip install --upgrade pip --no-cache-dir && pip install -r requirements.txt --no-cache-dir \
    && pip install --upgrade urllib3

# Копируем оставшееся файля. Для ускорения сборки образа эту команду стоит разместить ближе к концу файла.
COPY . .
#
ENV PYTHONPATH ${PYTHONPATH}:/opt/app/config

CMD [ "python3", "postgres_to_es/main.py"]
