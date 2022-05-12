import operator
from pyske.core import SStream
from pyske.core.util import fun
import os

def test_sstream():
    # pylint: disable=missing-docstring
    stream = SStream(os.getcwd()+"/pyske/test/stream/testdata_stream.txt", int, 10)
    while True:
        print(stream.getvaluefromsource())
        print(stream.filter(lambda val: val % 2 == 0))
        print(stream.map(fun.incr))
        print(stream.reduce(operator.add))
        stream.window()
test_sstream()
