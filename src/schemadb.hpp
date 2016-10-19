#ifndef SCHEMADB_H
#define SCHEMADB_H

#include <map>
#include <string>

#include "schema.hpp"

class SchemaDb {
public:
    SchemaDb(const std::string& filename);
    Schema get_schema(int id) const;
    void add_schema(const std::string& filename);

private:
    std::map<int, Schema> mapping;
    int next_id;
    std::string schemadb_filename;
};

#endif // SCHEMADB_H