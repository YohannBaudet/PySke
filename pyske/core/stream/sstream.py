"""
class SStream: sequential streams for real time data processing
"""

from typing import Generic, TypeVar, Callable, Optional
from pyske.core.list.slist import SList

__all__ = ['SStream']

T = TypeVar('T')  # pylint: disable=invalid-name
R = TypeVar('R')  # pylint: disable=invalid-name


class SStream(list, Generic[T]):
    """
        Sequential stream

        Methods:
            getlastwindowvalue, getdata, getvaluefromsource,
            map, filter, reduce, window, getwindow,
            setstream
        """

    def __init__(self, source: str, types: T, windowsize: int = 10):
        """
        Initialise the stream
        @param source: The path to the data file : str
        @param types: The type of the data that will be use in the stream : T
        @param windowsize: The size of the window : int
        """
        super().__init__()
        self.__type: T = types
        self.__source: str = source
        self.__window: SList = SList()
        self.__windowsize: int = windowsize
        self.__lastwindowvalue: T = None
        self.__data: SList = SList()
        self.__lastsourceline: int = 0
        self.__lastwindow: list = []

    def getlastwindowvalue(self) -> T:
        """
        @return: The last window reduce value : T
        """
        return self.__lastwindowvalue

    def getlastwindow(self) -> list:
        """
        @return: The list of the last window processed data
        """
        return self.__lastwindow

    def getdata(self) -> SList:
        """
        @return: The list of all data processed : SList
        """
        return self.__data

    def getvaluefromsource(self) -> SList:
        """
        Get the data from the file while the window hasn't reached
        its maximum size, then return the window
        @return: The list of data in the window : SList
        """
        i = 0
        while len(self.__window) < self.__windowsize:
            i += 1
            source = open(self.__source, 'r')
            file = source.readlines()
            newdata = []
            if len(file) > self.__lastsourceline:
                newdata = file[self.__lastsourceline: -1]
            for data in newdata:
                if len(self.__window) >= self.__windowsize:
                    break

                self.__lastsourceline += 1
                self.__window.append(self.__type(data))
            source.close()
        return self.__window

    def map(self, unary_op: Callable[[T], R]) -> SList:
        """
        Apply a function on all data of the window then return the window
        @param unary_op: A function (pyske.core.util.fun) to apply on all data : Callable[[T], R]
        @return: The list of processed data in the window : SList
        """
        self.__window = self.__window.map(unary_op)
        return self.__window

    def filter(self, predicate: Callable[[T], bool]):
        """
        Filter the value of a window with the predicate and return the window
        @param predicate: A function to filter all data in the window : Callable[[T]
        @return: The list of data in the window : SList
        """
        self.__window = self.__window.filter(predicate)
        return self.__window

    def reduce(self, binary_op: Callable[[T, T], T], neutral: Optional[T] = None) -> T:
        """
        Reduce all the data with an operator and return the value
        @param binary_op: The operator to use for the reduce : Callable[[T, T], T]
        @param neutral: Item place before the data for the calculation : Optional[T] = None
        @return: The value of the reduce : T
        """
        value = SList(self.__window)
        if self.__lastwindowvalue is not None:
            value.append(self.__lastwindowvalue)
        self.__lastwindowvalue = value.reduce(binary_op, neutral)
        return self.__lastwindowvalue

    def window(self):
        """
        Reset the window by adding the processed data to the __data list
        and clear the data on the window list
        """
        self.__data.extend(self.__window)
        self.__lastwindow = self.__window
        self.__window = SList()

    def getwindow(self) -> SList:
        """
        @return: The window containing the data : SList
        """
        return self.__window

    def setstream(self, window: SList, lastwindowvalue: T, data: SList):
        """
        Set the value of the stream with the parameter
        @param window: A list containing the data to process : SList
        @param lastwindowvalue: The value of the last reduce done in the last window : T
        @param data: The list of all data already process : SList
        """
        self.__window = window
        self.__lastwindowvalue = lastwindowvalue
        self.__data = data
