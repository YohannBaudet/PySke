import operator
from pyske.core import SStream
from pyske.core.util import fun

def test_stream():
    # pylint: disable=missing-docstring
    stream = SStream("/media/sf_TER/PySke/pyske/test/stream/testdata.txt", int, 100)
    while True:
        print(stream.getvaluefromsource())
        print(stream.filter(lambda val: val % 2 == 0))
        print(stream.map(fun.incr))
        print(stream.reduce(operator.add))
        stream.window()

test_stream()
