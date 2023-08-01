from engine.persistence.persistence import Persistence, State


class DummyPersistence(Persistence):
    def store(self, state: State):
        return
    
    def load(self, worker_id) -> State:
        return super().load(worker_id)