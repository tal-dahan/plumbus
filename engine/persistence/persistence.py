from abc import abstractmethod, ABC
from dataclasses import dataclass


@dataclass
class State:
    worker_id: int
    step_number: int
    inputs: dict


class Persistence(ABC):
    @abstractmethod
    def store(self, state: State):
        raise NotImplementedError()
    
    @abstractmethod
    def load(self, worker_id) -> State:
        raise NotImplementedError()
    
    def store_state(self, worker_id: int, step_number: int, inputs: dict):
        s = State(worker_id, step_number, inputs)
        return self.store(s)
    
    def load_state(self, worker_id) -> State:
        return self.load(worker_id)