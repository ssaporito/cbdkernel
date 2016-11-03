#ifndef SCHEMA_H
#define SCHEMA_H

#include <string>
#include <utility>
#include <vector>

#include "BPlusTree/bpt.h"
#include <unordered_map>

std::string get_current_timestamp();

class Schema {
public:
    Schema();
    virtual ~Schema();
    Schema(const std::string& filename, int id);
    int get_size() const;
    int get_header_size() const;
    int get_id() const;
    std::string get_filename() const;
    void convert_to_bin(const std::string& csv_filename, const std::string& bin_filename, bool ignore_first_line = true) const;
    void print_binary(const std::string& bin_filename) const;
    void create_index(const std::string& bin_filename, const std::string& index_filename) const;
    void create_index_bplus(const std::string& bin_filename, const std::string& index_filename) const;
    void create_index_hash(const std::string& bin_filename, const std::string& index_filename) const;
    void create_index_direct_hash(const std::string& csv_filename, const std::string& bin_filename, bool ignore_first_line) const;
    void create_index_indirect_hash(const std::string& bin_filename, const std::string& index_filename) const;
    void load_index(const std::string& index_filename);
    void load_index_bplus(const std::string& index_filename);
    void load_index_indirect_hash(const std::string& index_filename);
    int search_for_key(int key) const;
    int search_for_key_bplus(int key) const;
    int search_for_key_indirect_hash(int key) const;
    int search_for_key_direct_hash(int key, const std::string& bin_filename) const;
    int search_for_key_raw(int key, const std::string& bin_filename) const;
    std::vector<int> search_field(std::string field_name, std::string field_value, const std::string& bin_filename, int init_pos) const;
    void join_existing_index(const Schema &schema2);
    void join_new_index(const Schema &schema2);
    void join_merge(Schema &schema2);
    void join_hash(const Schema &schema2);
    void load_data(int pos, const std::string& bin_filename);
    static const int TIMESTAMP_SIZE = 25;
    static const int HEADER_SIZE = TIMESTAMP_SIZE * sizeof(char) + 2 * sizeof(int);
    bpt::bplus_tree *bplus = NULL;
    
private:
    void compute_size();
    void compute_header_size();
    std::vector< std::pair<std::string, std::string> > metadata;
    int size;
    int header_size;
    std::string schema_filename;
    int id;
    std::vector<std::pair<int, int> > index_map;
    std::unordered_map<std::size_t*,int> index_hash;

};

#endif // SCHEMA_H
