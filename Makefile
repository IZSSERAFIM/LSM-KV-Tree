
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

init = memtable.o sstable.o

all: correctness persistence

correctness: kvstore.o correctness.o $(init)
persistence: kvstore.o persistence.o $(init)

clean:
	-rm -f correctness persistence *.o
	-rm -f ./data/*.sst
	-rm -f ./data/vlog

