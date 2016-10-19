#ifdef INDEX_H
#define INDEX_H

#include <vector>

class Index {

public:

    Index(std::string filename);
    void add_entry(int key, int offset);

private:

    std::string index_filename;

    // map from index to file offset
    std::vector<std::pair<int, int> > keys; 

}

#endif // !INDEX_H


/* Problems: how to store an index file for a schema?
*/