from pyske.slist import SList
from pyske.plist import PList
from pyske.support.parallel import *
import sys
import random
from mpi4py import MPI

# Generating a parallel list of the size specified on the command line or 1000
if len(sys.argv) > 1:
    size = int(sys.argv[1])
else:
    size = 1000
X = PList.init(lambda _: random.randint(0,100), size)

# start timing
t = PList.init(lambda _: time(), nprocs)

# computing the variance
add = lambda x,y: x+y
n = X.length()
avg = X.reduce(add) / n
var = (X.map(lambda x: (x-avg)**2).reduce(add)**0.5) / n

# stop timing
elapsed = t.map(lambda x: time()-x)
max_elapsed = elapsed.reduce(max)
avg_elapsed = elapsed.reduce(add) / nprocs
all_elapsed = elapsed.mapi(lambda i,x: "["+str(i)+"]:"+str(x)).to_seq()  

# ouput at processor 0
at_root(lambda: print("Variance: ",var,"\nTime (max):",max_elapsed,"\nTime (avg):",avg_elapsed, "\nTime (all):",all_elapsed))
