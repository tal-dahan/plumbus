from typing import List
from threading import Thread, RLock
from datetime import datetime
from engine.core import Pipeline, IdGenerator
from enum import Enum, auto
from engine.persistence.persistence import Persistence
from engine.persistence.file_persistence import FileBackup


class WorkerStatus(Enum):
    NOT_STARTED = auto()
    RUNNING = auto()
    STOPPED = auto()
    STOPPING = auto()
    DONE = auto()
    FAILED = auto()


class PipelineWorker(Thread):
    def __init__(self, pipeline: Pipeline, persistence: Persistence, step: int = 0, data = {}) -> None:
        super().__init__()
        self.__id = IdGenerator.generate('pipeline')
        self.__pipeline = pipeline
        self.__input = data
        self.__output = {}
        self.__current_step = step
        self.__status = WorkerStatus.NOT_STARTED
        self.__stop_flag = False
        self.__stop_lock = RLock()
        self.__start_time = "n/a"
        self.__persistence = persistence

    @property
    def id(self):
        return self.__id

    @property
    def pipeline(self):
        return self.__pipeline
    
    @property
    def status(self):
        return self.__status
    
    @property
    def current_step(self):
        return self.__current_step
    
    @property
    def output(self):
        return self.__output
    
    @property
    def is_stopped(self):
        with self.__stop_lock:
            return self.__stop_flag

    def __run_step(self, step, idx, data):
        try:
            return step(data)
        except Exception as err:
            raise Exception(f"pipeline '{self.__pipeline.name}' failed in step {idx + 1}: {err.__repr__()}")
            
    def __run_step_with_retires(self, step, idx, data):
        try_count = 0
        last_error = None

        while try_count < step.retries:
            try:
                return self.__run_step(step, idx, data)
            except Exception as err:
                last_error = err
                try_count += 1
            
            raise last_error
        
    def start(self, *args, **kwargs):
        self.__status = WorkerStatus.RUNNING
        self.__start_time = datetime.now()
        super().start(*args, **kwargs)

    def run(self):
        step_result = self.__input

        for idx, step in enumerate(self.__pipeline.steps, self.__current_step):
            if self.is_stopped:
                self.__status = WorkerStatus.STOPPED
                self.__output = step_result
                self.__persistence.store_state(state=self.id, step_number=self.__current_step, inputs=step_result)
                return
            else:
                try:
                    step_result = self.__run_step(step, idx, step_result)
                except Exception as err:
                    self.__status = WorkerStatus.FAILED
                    self.__output = {'err': str(err)}
                    return
                else:
                    self.__current_step += 1
                    self.__persistence.store_state(worker_id=self.id, step_number=self.__current_step, inputs=step_result)

        self.__status = WorkerStatus.DONE
        self.__output = step_result

    def stop(self):
        with self.__stop_lock:
            self.__status = WorkerStatus.STOPPING
            self.__stop_flag = True


class Orchestrator:
    def __init__(self, pipelines: List[Pipeline]) -> None:
        self.__pipelines: List[Pipeline] = pipelines
        self.__workers: List[PipelineWorker] = []
        self.__persistence = FileBackup(".")

    def __run(self, pipeline, step, inputs):
        worker = PipelineWorker(pipeline, self.__persistence, inputs)
        self.__workers.append(worker)
        worker.start()

        return worker.id

    def run_pipeline(self, name: str, data = {}) -> int:
        pipeline = self.find_pipeline(name)
        return self.__run(pipeline=pipeline, step=0, inputs=data)
    
    def run_worker(self, worker_id):
        state = self.__persistence.load_state(worker_id)
        worker = self.find_worker(worker_id)
        return self.__run(pipeline=worker.pipeline, step=state.step_number, inputs=state.inputs)

    def find_pipline_by_name(self, name):
        try:
            return next(pipeline for pipeline in self.__pipelines if pipeline.name == name)
        except:
            raise Exception(f"no pipeline named '{name}' exists")
        
    def get_workers(self) -> List[PipelineWorker]:
        # return [(worker.id, worker.pipeline.name, worker.status) for worker in self.__workers]
        return [worker for worker in self.__workers]
    
    def get_workers_by_pipeline(self, pipeline_name):
        return [(worker.id, worker.pipeline.name) for worker in self.__workers if worker.pipeline.name == pipeline_name]

    def find_worker(self, id) -> PipelineWorker:
        try:
            return next(worker for worker in self.__workers if worker.id == id)
        except:
            raise Exception(f"no worker with id '{id}' exists")

    def get_pipelines(self):
        return [p.name for p in self.__pipelines]
 
    def find_pipeline(self, name) -> Pipeline:
        try:
            return next(pipeline for pipeline in self.__pipelines if pipeline.name == name)
        except:
            raise Exception(f"there is no pipeline named '{name}'")

    def stop_worker(self, id):
        worker = self.find_worker(id)
        worker.stop()

    def remove_worker(self, id):
        worker = self.find_worker(id)
        
        if worker.status == WorkerStatus.RUNNING:
            raise Exception("can't remove a running worker")
        else:
            self.__workers.remove(worker)


# stubssssssssssssss

def step1(e):
    import time
    e['str'] = e['str'] + "1"
    time.sleep(10)
    return e


def step2(e):
    e['str'] = e['str'] + "2"
    return e

def generate_dummy_pipelines():
    p1 = Pipeline('lol-pipe')
    p1.step(step1)
    p1.step(step2)

    @p1.step
    def step3(e):
        # raise Exception("asd")
        e['str'] = e['str'] + '3'
        return e
    
    return [p1]


if __name__ == "__main__":
    pipes = generate_dummy_pipelines()
    orcha = Orchestrator(pipelines=pipes)

    orcha.run_pipeline("lol-pipe", {'str': 'lol'})
    print(1)