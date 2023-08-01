from typing import List


class Step:
    def __init__(self, callback, retries = 1) -> None:
        self.__callback = callback
        self.__retries = retries

    @property
    def retries(self):
        return self.__retries

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        return self.__callback(*args, **kwargs)


class Pipeline:
    def __init__(self, name, steps = []) -> None:
        self.__name = name
        self.__steps : List[Step] = steps

    @property
    def name(self):
        return self.__name
    
    def step(self, func):
        s = Step(func)
        self.__steps.append(s)
        def wrapper(data):
            return s(data)
        return wrapper

    @property
    def steps(self):
        return self.__steps
    

class IdGenerator:
    __IDS__ = {}

    @staticmethod
    def generate(topic):
        id = IdGenerator.__IDS__.get(topic, 0)
        IdGenerator.__IDS__[topic] = id + 1
        
        return IdGenerator.__IDS__[topic]
    
    @staticmethod
    def seed(topic, value):
        IdGenerator.__IDS__[topic] = value
