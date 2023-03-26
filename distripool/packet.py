import multiprocessing
from dataclasses import dataclass
from typing import Literal, List


@dataclass
class DataPacket:
    id: int
    func: any
    chunk: List[any]
    mapping_type: Literal['map', 'starmap'] = 'map'
    initializer: any = None
    initargs: any = None
    maxtasksperchild: int | None = None

    def choose_mapping(self, pool: multiprocessing.Pool):
        match self.mapping_type:
            case 'map': return pool.map
            case 'starmap': return pool.starmap


@dataclass
class ResultPacket:
    id: int
    result: any

    def holds_error(self) -> bool:
        return isinstance(self.result, Exception)
