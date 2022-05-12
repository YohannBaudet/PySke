import operator
from pyske.core import PStream
from pyske.core.util import fun

def test_pstream():
    # pylint: disable=missing-docstring
    stream = PStream("/media/sf_TER/PySke/pyske/test/stream/testdata_stream.txt", int, 10)
    stream.filter(lambda val: val % 2 == 0)
    stream.map(fun.incr)
    stream.reduce(operator.add)
    stream.run()

    while True:
        print(stream.getdata())
        print(stream.getlastwindowvalue())

test_pstream()
