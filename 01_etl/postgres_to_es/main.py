import datetime
import time
from functools import wraps

import elasticsearch
from elasticsearch import Elasticsearch
import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import DictCursor
import logging
from dotenv import load_dotenv
import os
from state import state

import json


logging.basicConfig(filename="es.log", level=logging.INFO)

es = Elasticsearch(hosts="http://localhost:9200/")
logging.info(es.ping())


def create_index(index_name: str) -> None:
    """Создание ES индекса.
       :param index_name: Наименование индекса.
       :param mapping: Настройки индекса
    """
    mapping = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "imdb_rating": {
                    "type": "float"
                },
                "genre": {
                    "type": "keyword"
                },
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "director": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "writers_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                }
            },
        },
    }
    logging.info(f"Создание индекса {index_name} со следующими схемами:{json.dumps(mapping, indent=2)}")
    es.indices.create(index=index_name, body=mapping)


if not es.indices.exists(index='movies'):
    create_index('movies')
else:
    logging.info("Index is already created.")


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            tries = 0
            while True:
                t = min(border_sleep_time, start_sleep_time * factor ** tries)
                if tries > 0:
                    time.sleep(t)
                try:
                    return func(*args, **kwargs)
                except (
                        psycopg2_errors.ConnectionException,
                        psycopg2_errors.SqlclientUnableToEstablishSqlconnection,
                        psycopg2_errors.ConnectionFailure,
                        psycopg2_errors.TransactionResolutionUnknown,
                        psycopg2_errors.InvalidTransactionState,
                        elasticsearch.TransportError, elasticsearch.ConnectionTimeout,
                        elasticsearch.ConnectionError
                ) as e:
                    tries += 1
                    logging.error(str(e))

        return inner

    return func_wrapper


load_dotenv('.env')
dsl = {'dbname': os.environ.get('dbname'), 'user': os.environ.get('user'),
       'password': os.environ.get('password'), 'host': os.environ.get('host'),
       'port': os.environ.get('port')}


class Extractor:
    def __init__(self, dsl, chunk_size):
        self.connection = psycopg2.connect(**dsl)
        self.chunk_size = chunk_size

    @backoff()
    def extract(
            self, extract_timestamp: datetime.datetime,
            start_timestamp: datetime.datetime,
            exclude_ids: list
    ):
        """Читаем данные пачками. В случае падения начинаем читать с последней обработанной записи"""
        with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            sql = """
            SELECT fw.id,
            fw.rating as imdb_rating, 
            json_agg(DISTINCT g.name) as genre,
            fw.title,
            fw.description,
            string_agg(DISTINCT CASE WHEN pfw.role = 'director' THEN p.full_name ELSE '' END, ',') AS director,
            array_remove(COALESCE(array_agg(DISTINCT CASE WHEN pfw.role = 'actor' THEN p.full_name END) FILTER (WHERE p.full_name IS NOT NULL)), NULL) AS actors_names,
            array_remove(COALESCE(array_agg(DISTINCT CASE WHEN pfw.role = 'writer' THEN p.full_name END) FILTER (WHERE p.full_name IS NOT NULL)), NULL) AS writers_names,
            concat('[', string_agg(DISTINCT CASE WHEN pfw.role = 'actor' THEN json_build_object('id', p.id, 'name', p.full_name) #>> '{}' END, ','), ']') AS actors,
            concat('[', string_agg(DISTINCT CASE WHEN pfw.role = 'writer' THEN json_build_object('id', p.id, 'name', p.full_name) #>> '{}' END, ','), ']') AS writers,
            GREATEST(MAX(fw.modified), MAX(g.modified), MAX(p.modified)) AS last_modified
            FROM content.film_work as fw
            LEFT JOIN content.genre_film_work gfm ON fw.id = gfm.film_work_id
            LEFT JOIN content.genre g ON gfm.genre_id = g.id
            LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
            LEFT JOIN content.person p ON pfw.person_id = p.id
            GROUP BY fw.id
            HAVING GREATEST(MAX(fw.modified), MAX(g.modified), MAX(p.modified)) > %(extract_timestamp)s
            """
            if exclude_ids:
                sql += """
                AND (fw.id not in %(exclude_ids)s OR 
                  GREATEST(MAX(fw.modified), MAX(g.modified), MAX(p.modified)) > %(start_timestamp)s)
                """
            sql += """
            ORDER BY last_modified DESC;
            """
            cursor.execute(
                sql, {
                    'exclude_ids': tuple(exclude_ids), 'extract_timestamp': extract_timestamp,
                    'start_timestamp': start_timestamp
                }
            )

            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    cursor.close()
                    break
                for data in rows:
                    ids_list = state.get_state("filmwork_ids")
                    ids_list.append(data['id'])
                    state.set_state("filmwork_ids", ids_list)
                yield rows


class Transformer:
    def transform(self, data):
        """Обработка данных из Postgres и преобразование в формат для ElasticSearch"""
        butch = []
        for row in data:
            filmwork = {
                "id": row['id'],
                "imdb_rating": row['imdb_rating'],
                "genre": row['genre'],
                "title": row['title'],
                "description": row['description'],
                "director": row['director'],
                "actors_names": row['actors_names'] if row['actors_names'] is not None else '',
                "writers_names": row['writers_names'] if row['writers_names'] is not None else '',
                "actors": json.loads(row['actors']) if row['actors'] is not None else [],
                "writers": json.loads(row['writers']) if row['writers'] is not None else []
            }
            butch.append(filmwork)
        return butch


class Loader:
    @backoff()
    def load(self, data):
        """Загружаем данные пачтами в ElasticSearch"""
        for row in data:
            response = es.index(
                index='movies',
                doc_type='_doc',
                id=row['id'],
                document=row,
                request_timeout=45
            )
            logging.info(response)


class ETL:

    def __init__(
            self, extractor: Extractor,
            transformer: Transformer,
            loader: Loader
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):
        start_timestamp = datetime.datetime.now()
        for butch in self.extractor.extract(
            extract_timestamp=state.get_state('last_sync_timestamp'),
            start_timestamp=start_timestamp,
            exclude_ids=state.get_state('filmwork_ids')
        ):
            data = self.transformer.transform(butch)
            self.loader.load(data)
        state.set_state("filmwork_ids", [])
        state.set_state("last_sync_timestamp", str(start_timestamp))


if __name__ == "__main__":
    etl = ETL(Extractor(dsl, 50), Transformer(), Loader())
    etl.run()

