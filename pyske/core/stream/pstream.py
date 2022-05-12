"""
class SStream: parallel streams for real time data processing.
"""

from typing import Generic, TypeVar, Callable, Optional
from pyske.core.stream.sstream import SStream
from pyske.core.support import parallel as parimpl
from pyske.core.list.slist import SList

__all__ = ['PStream']

_PID: int = parimpl.PID
_NPROCS: int = parimpl.NPROCS
_COMM = parimpl.COMM

T = TypeVar('T')  # pylint: disable=invalid-name
R = TypeVar('R')  # pylint: disable=invalid-name


class PStream(list, Generic[T]):
    """
    Parallel stream

    Is used to automize the process of getting the data from the file in the root proc
    this allows to use the others' proc in the main

    Methods:
            getvaluefromsource, stop, mapr, filterr,
            reducer, map, reduce, filter, dooperation,
            bcaststream, getdata, getlastwindowvalue,
            getoperations, getwindow, run
    """

    def __init__(self, source: str, types: T, windowsize: int = 10):
        """
        Initialise the stream
        @param source: The path to the data file : str
        @param types: The type of the data that will be use in the stream : T
        @param windowsize: The size of the window : int
        """
        super().__init__()
        self.__stream: SStream = SStream(source, types, windowsize)
        self.__stop: bool = False
        self.__operations: list = []

    def getvaluefromsource(self):
        """
        Get the data from the file while the window hasn't reached
        its maximum size, then run all the operation and broadcast
        the all the value to the other proc
        """
        self.__stop: bool = False
        while not self.__stop:
            self.__stream.getvaluefromsource()
            self.dooperation()
            self.bcaststream()

    def getlastwindow(self) -> T:
        """
        @return: The last window reduce value : T
        """
        return self.__stream.getlastwindow()

    def stop(self):
        """
        Allow stopping running the getvaluefromsource methode
        """
        self.__stop = True

    def mapr(self, unary_op: Callable[[T], R]) -> SList:
        """
        Apply a function on all data of the window then return the window
        @param unary_op: A function (pyske.core.util.fun) to apply on all data : Callable[[T], R]
        @return: The list of processed data in the window : SList
        """
        return self.__stream.map(unary_op)

    def filterr(self, predicate: Callable[[T], bool]) -> SList:
        """
        Filter the value of a window with the predicate and return the window
        @param predicate: A function to filter all data in the window : Callable[[T]
        @return: The list of data in the window : SList
        """
        return self.__stream.filter(predicate)

    def reducer(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None) -> T:
        """
        Reduce all the data with an operator and return the value
        @param binary_op: The operator to use for the reduce : Callable[[T, T], T]
        @param neutral: Item place before the data for the calculation : Optional[T] = None
        @return: The value of the reduce : T
        """
        return self.__stream.reduce(binary_op, neutral)

    def map(self, unary_op: Callable[[T], R]) -> SList:
        """
        Add the map function to the operations list to execute
        once the window is full
        @param unary_op: A function (pyske.core.util.fun) to apply on all data : Callable[[T], R]
        @return: The list of data in the window : SList
        """
        self.__operations.append([self.mapr, unary_op])
        return self.__stream.getwindow()

    def filter(self, predicate: Callable[[T], bool]) -> SList:
        """
        Add the filter function to the operations list to execute
        once the window is full
        @param predicate: A function to filter all data in the window : Callable[[T]
        @return: The list of data in the window : SList
        """
        self.__operations.append([self.filterr, predicate])
        return self.__stream.getwindow()

    def reduce(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None) -> T:
        """
        Add the reduce function to the operations list to execute
        once the window is full
        @param binary_op: The operator to use for the reduce : Callable[[T, T], T]
        @param neutral: Item place before the data for the calculation : Optional[T] = None
        @return: The value of the reduce : T
        """
        self.__operations.append([self.reducer, binary_op, neutral])
        return self.__stream.getlastwindowvalue()

    def dooperation(self):
        """
        Run all the operations to execute on the window then create a new window
        """
        for operation in self.__operations:
            if len(operation) == 2:
                operation[0](operation[1])
            if len(operation) == 3:
                operation[0](operation[1], operation[2])
        self.__stream.window()

    def bcaststream(self):
        """
        Broadcast the value of the stream of the root to the other stream
        Values usefull to broadcast:
            - window : The data in the window
            - lastwindowvalue : The value of the last reduce
            - data : All the data processed
        """
        self.__stream = _COMM.bcast(self.__stream, 0)

    def getdata(self) -> SList:
        """
        Broadcast the value of the stream from the root to the other proc
        and then return all the data processed
        @return: The data processed : SList
        """
        self.bcaststream()
        return self.__stream.getdata()

    def getlastwindowvalue(self) -> T:
        """
        Broadcast the value of the stream from the root to the other proc
        and then return the value of the last reduce
        @return: The data processed : SList
        """
        self.bcaststream()
        return self.__stream.getlastwindowvalue()

    def getoperations(self) -> list:
        """
        @return: The list of all operation to run after the window is full
        """
        return self.__operations

    def getwindow(self):
        """
        @return: The window containing the data : SList
        """
        return self.__stream.getwindow()

    def run(self):
        """
        Run the getter of the data from the file and the execution
        of the operation once the window is full
        """
        if _PID == 0:
            self.getvaluefromsource()
