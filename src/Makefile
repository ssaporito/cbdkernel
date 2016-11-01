CXXFLAGS = -Wall -Wextra -Wno-unused-parameter -std=c++11
SOURCES=$(wildcard *.cpp)
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=db

all: $(EXECUTABLE)

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(EXECUTABLE): $(OBJECTS) BPlusTree/bpt.cc
	$(CXX) $(CXXFLAGS) $^ -o $@

clean:
	$(RM) $(OBJECTS) $(EXECUTABLE)

.PHONY: clean all
