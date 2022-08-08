import abc
from typing import Any, Optional
import json
from json import JSONDecodeError
import os


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        if not os.path.exists(file_path):
            with open(self.file_path, 'w+') as json_file:
                init_state = {
                    'last_sync_timestamp': '0001-01-01 00:00:00',
                    'filmwork_ids': []
                }
                json.dump(init_state, json_file)

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w+') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self) -> dict:
        json_object = {}
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r+') as json_file:
                try:
                    json_object = json.load(json_file)
                except JSONDecodeError:
                    pass
        else:
            print('Файл не существует')
        return json_object


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state_dict = self.storage.retrieve_state()
        state_dict.update({key: value})
        self.storage.save_state(state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state_dict = self.storage.retrieve_state()
        return state_dict.get(key, None)
