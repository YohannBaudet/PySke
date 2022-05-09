"""
class SStream: sequential streams.
"""

from typing import Generic, TypeVar, Callable, Optional
from pyske.core.list.slist import SList

__all__ = ['SStream']

T = TypeVar('T')  # pylint: disable=invalid-name
R = TypeVar('R')  # pylint: disable=invalid-name

class SStream(list, Generic[T]):

    def __init__(self, source: str, type, windowsize: int = 10):
        super().__init__()
        self.__type = type
        self.__source: str = source
        self.__window: SList = SList()
        self.__windowsize: int = windowsize
        self.__lastwindowvalue: T = None
        self.__lastwindow: list = []
        self.__lastsourceline: int = 0

    def getlastwindowvalue(self):
        return self.__lastwindowvalue

    def getvaluefromsource(self):
        i = 0
        while len(self.__window)<self.__windowsize:
            i+=1
            source = open(self.__source, 'r')
            file = source.readlines()
            newdata = []
            if len(file) > self.__lastsourceline:
                newdata = file[self.__lastsourceline: -1]
            for data in newdata:
                if len(self.__window)>=self.__windowsize:
                    break

                self.__lastsourceline+=1
                self.__window.append(self.__type(data))
            source.close()
        return self.__window

    def map(self, unary_op: Callable[[T], R]):
        self.__window = self.__window.map(unary_op)
        return self.__window

    def filter(self, predicate: Callable[[T], bool]):
        self.__window = self.__window.filter(predicate)
        return self.__window

    def reduce(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None):
        value = SList(self.__window)
        if self.__lastwindowvalue is not None:
            value.append(self.__lastwindowvalue)
        self.__lastwindowvalue = value.reduce(binary_op, neutral)
        return self.__lastwindowvalue

    def window(self):
        self.__lastwindow = SList(self.__window)
        self.__window = SList()




        
