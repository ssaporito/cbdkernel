#include <iostream>

#include "benchmark.hpp"    

void search_range(const Schema &schema, int lowlimit, int highlimit) {
    for(int i = lowlimit; i < highlimit; ++i ) schema.search_for_key(i);
}

void search_range_bplus(const Schema &schema, int lowkey, int highkey) {
    int size = (highkey - lowkey) + 1;
    bool next;
    bpt::value_t *values = new bpt::value_t[size];
    bpt::key_t lk = bpt::key_t(std::to_string(lowkey).c_str());
    schema.bplus->search_range(&lk, bpt::key_t(std::to_string(highkey).c_str()), values, size, &next);
    delete [] values;
}

void search_range_raw(const Schema &schema, int lowkey, int highkey, const std::string &filename) {
    for(int i = lowkey; i < highkey; ++i ) schema.search_for_key_raw(i, filename);
}

void search_set(const Schema &schema, std::vector<int> set) {
    for(const auto& element: set) schema.search_for_key(element);
}

void search_set_bplus(const Schema &schema, std::vector<int> set) {
    for(const auto& element: set) schema.search_for_key_bplus(element);
}

void search_set_raw(const Schema &schema, std::vector<int> set, const std::string &filename) {
    for(const auto& element: set) schema.search_for_key_raw(element, filename);
}