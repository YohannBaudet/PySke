"""
class SStream: parallel streams.
"""

from typing import Generic, TypeVar, Callable, Optional
from pyske.core.stream.sstream import SStream
from pyske.core.support import parallel as parimpl

__all__ = ['PStream']

_PID: int = parimpl.PID
_NPROCS: int = parimpl.NPROCS
_COMM = parimpl.COMM

T = TypeVar('T')  # pylint: disable=invalid-name
R = TypeVar('R')  # pylint: disable=invalid-name

class PStream(list, Generic[T]):

    def __init__(self, source: str, types: T, windowsize: int = 10):
        super().__init__()
        self.__stream : SStream = SStream(source, types, windowsize)
        self.__stop : bool = False
        self.__operations : list = []

    def getvaluefromsource(self):
        self.__stop: bool = False
        while not self.__stop:
            self.__stream.getvaluefromsource()
            self.dooperation()
            self.bcaststream()


    def stop(self):
        self.__stop=True

    def mapr(self, unary_op: Callable[[T], R]):
        return self.__stream.map(unary_op)

    def filterr(self, predicate: Callable[[T], bool]):
        return self.__stream.filter(predicate)

    def reducer(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None):
        return self.__stream.reduce(binary_op, neutral)

    def map(self, unary_op: Callable[[T], R]):
        self.__operations.append([self.mapr, unary_op])
        return self.__stream.getwindow()

    def filter(self, predicate: Callable[[T], bool]):
        self.__operations.append([self.filterr, predicate])
        return self.__stream.getwindow()

    def reduce(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None):
        self.__operations.append([self.reducer, binary_op, neutral])
        return self.__stream.getlastwindowvalue()

    def dooperation(self):
        for operation in self.__operations:
            if len(operation) == 2:
                operation[0](operation[1])
            if len(operation) == 3:
                operation[0](operation[1], operation[2])
        self.__stream.window()

    def bcaststream(self):
        self.__stream = _COMM.bcast(self.__stream, 0)

    def getdata(self):
        self.bcaststream()
        return self.__stream.getdata()

    def getlastwindowvalue(self):
        self.bcaststream()
        return self.__stream.getlastwindowvalue()

    def getoperations(self):
        return self.__operations

    def getwindow(self):
        return self.__stream.getwindow()

    def run(self):
        if _PID==0:
            self.getvaluefromsource()

