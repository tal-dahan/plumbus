from engine.persistence.persistence import Persistence, State
import json
import os
import shutil


class FileBackup(Persistence):
    def __init__(self, path: str) -> None:
        self.__path = path
    
    def __store_step(self, base_path: str, step_number: int):
        step_file_name = f"{base_path}\\step" 

        with open(step_file_name, 'wb') as f:
            f.write(str(step_number).encode())

    def __store_inputs(self, base_path: str, inputs: dict):
        dump = json.dumps(inputs)
        inputs_file_name = f"{base_path}\\inputs"

        with open(inputs_file_name, 'wb') as f:
            f.write(dump.encode())

    def store(self, state: State):
        base_path = os.path.join(self.__path, "backup", str(state.worker_id))
        os.makedirs(base_path, exist_ok=True)

        try:
            self.__store_step(base_path, state.step_number)
            self.__store_inputs(base_path, state.inputs)
        except Exception as e:
            shutil.rmtree(base_path, ignore_errors=True)
            return Exception("failed to store state")

    def __load_step(self, base_path: str):
        step_file_name = f"{base_path}\\step" 

        with open(step_file_name, 'rb') as f:
            return int(f.read().decode())

    def __load_inputs(self, base_path: str):
        inputs_file_name = f"{base_path}\\inputs"

        with open(inputs_file_name, 'rb') as f:
            return json.loads(f.read().decode())

    def load(self, worker_id) -> State:
        base_path = os.path.join(self.__path, "backup", str(state.worker_id))
        step = self.__load_step(base_path)
        inputs = self.__load_inputs(base_path)

        return State(worker_id, step, inputs)
    
# ID = 19773618923
# b = FileBackup(".")
# print(b)
# state = State(ID, 84, {'str':'lol2'})
# b.store(state)
# b2 = b.load(ID)
# print(b2)