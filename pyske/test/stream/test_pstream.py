import operator
from pyske.core import PStream
from pyske.core.util import fun
import os


def test_pstream():
    # pylint: disable=missing-docstring
    stream = PStream(os.getcwd() + "/pyske/test/stream/testdata_stream.txt", int, 10)
    stream.filter(lambda val: val % 2 == 0)
    stream.map(fun.incr)
    stream.reduce(operator.add)
    stream.run()

    while True:
        (stream.getlastwindow())
        (stream.getlastwindowvalue())


test_pstream()
