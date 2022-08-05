from .main import JsonFileStorage, State

storage = JsonFileStorage(file_path='state.json')
state = State(storage)
