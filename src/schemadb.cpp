#include "schemadb.hpp"

#include <algorithm>
#include <fstream>

SchemaDb::SchemaDb(const std::string& filename) :
    schemadb_filename(filename) {
    next_id = -1;
    std::ifstream input(filename);

    while(input.good() && input.peek() != EOF) {
        std::string id_string, schema_filename;
        std::getline(input, id_string, ',');
        std::getline(input, schema_filename);

        int id = std::stoi(id_string);
        next_id = std::max(next_id, id);

        mapping[id] = Schema(schema_filename, id);
    }

    ++next_id;

    input.close();
}

Schema SchemaDb::get_schema(int id) const {
    return mapping.at(id);
}

void SchemaDb::add_schema(const std::string& filename) {
    Schema schema(filename, next_id);
    mapping[next_id] = schema;

    std::ofstream output(schemadb_filename, std::ofstream::app);
    output << next_id << "," << schema.get_filename() << std::endl;
    output.close();

    ++next_id;
}