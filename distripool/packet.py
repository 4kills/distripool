import multiprocessing
from dataclasses import dataclass
from typing import Literal, List


@dataclass
class DataPacket:
    id: int
    func: str
    func_name: str
    chunk: List[any]
    mapping_type: Literal['map', 'starmap'] = 'map'
    initializer: any = None
    initargs: any = None
    maxtasksperchild: int | None = None


@dataclass
class ResultPacket:
    id: int
    result: any

    def holds_error(self) -> bool:
        return isinstance(self.result, Exception)
