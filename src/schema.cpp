#include "schema.hpp"

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <iostream>
#include <unordered_map>

// Format Www Mmm dd hh:mm:ss yyyy
std::string get_current_timestamp() {
    time_t rawtime;
    struct tm* timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    return asctime(timeinfo);
}

Schema::Schema(){
    compute_size();
    compute_header_size();
}

Schema::~Schema() {
    if(!bplus) {
        delete bplus;
    }
}

Schema::Schema(const std::string& filename, int id) :
    schema_filename(filename),
    id(id) {
    std::ifstream input(filename);

    while(input.good() && input.peek() != EOF) {
        std::string datatype, column;
        std::getline(input, datatype, ',');
        std::getline(input, column);

        metadata.push_back(std::make_pair(datatype, column));
    }

    input.close();

    compute_size();
    compute_header_size();
}

int Schema::get_size() const {
    return size;
}

int Schema::get_header_size() const {
    return header_size;
}

void Schema::compute_size() {
    size = 0;

    for(const auto& data: metadata) {
        if (data.first[0] == 'i') {
            size += sizeof(int);
        }
        else {
            // ASSUMES: string.
            size += atoi(data.first.c_str() + 1);
        }
    }
}

void Schema::compute_header_size(){
    header_size=(1+1)*sizeof(int)+TIMESTAMP_SIZE*sizeof(char);
}

int Schema::get_id() const {
    return id;
}

std::vector< std::pair<std::string, std::string> > Schema::get_metadata(){
    return metadata;
}
std::vector<std::pair<int, int> > Schema::get_index_map(){
    return index_map;
}
std::unordered_map<std::size_t*,int> Schema::get_index_hash(){
    return index_hash;
}

std::string Schema::get_filename() const {
    return schema_filename;
}

void Schema::convert_to_bin(const std::string& csv_filename, const std::string& bin_filename, bool ignore_first_line) const {
    std::ifstream csv_file(csv_filename);
    FILE* bin_file = fopen(bin_filename.c_str(), "wb");

    if(ignore_first_line) {
        std::string line;
        std::getline(csv_file, line);
    }

    int next_key = 0;

    while(csv_file.good() && csv_file.peek() != EOF) {
        std::string timestamp = get_current_timestamp();

        // Write header.
        fwrite(&next_key, sizeof(int), 1, bin_file);
        fwrite(timestamp.c_str(), sizeof(char), TIMESTAMP_SIZE, bin_file);
        fwrite(&id, sizeof(int), 1, bin_file);
        ++next_key;

        // Write data.
        for(unsigned i = 0; i < metadata.size(); ++i) {
            const auto& data = metadata[i];
            const char delimiter = (i == (metadata.size() - 1)) ? '\n' : ',';

            std::string token;
            std::getline(csv_file, token, delimiter);

            if (data.first[0] == 'i') {
                const int int_token = std::stoi(token);
                fwrite(&int_token, sizeof(int), 1, bin_file);
                std::cout<<int_token<<std::endl;
            }
            else {
                // ASSUMES: string.
                fwrite(token.c_str(), sizeof(char), atoi(data.first.c_str() + 1), bin_file);
            }
        }
    }

    fclose(bin_file);
    csv_file.close();
}

void Schema::print_binary(const std::string& bin_filename) const{
    FILE* bin_file = fopen(bin_filename.c_str(),"rb");
    int offset=get_header_size();
    fseek(bin_file,0,SEEK_SET);

    while(!feof(bin_file)){
        fseek(bin_file,offset,SEEK_CUR);
        for(unsigned i = 0; i < metadata.size() ; i++){
            if (metadata[i].first[0] == 'i') {
                int int_token;
                fread(&int_token, sizeof(int), 1, bin_file);
                std::cout<<int_token;
            }
            else{
                int string_size=atoi(metadata[i].first.c_str()+1);
                char* data_value=(char*)malloc(sizeof(char)*string_size);
                fread(data_value,sizeof(char),string_size,bin_file);  
                std::cout<<data_value;  
            }  
            if(i==metadata.size()-1){
                std::cout<<"\n"<<std::endl;
            }   
            else{
                std::cout<<",";
            }                       
        }        
    }
    
    fclose(bin_file);
}

void Schema::create_index(const std::string& bin_filename, const std::string& index_filename) const {
    FILE* bin_file = fopen(bin_filename.c_str(), "rb");
    FILE* index_file = fopen(index_filename.c_str(), "wb");

    int offset = 0;
    int pace = HEADER_SIZE + size - sizeof(int);
    int key;

    while(fread(&key, sizeof(int), 1, bin_file)) {
        fwrite(&key, sizeof(int), 1, index_file);
        fwrite(&offset, sizeof(int), 1, index_file);

        offset += pace;
        fseek(bin_file, pace, SEEK_CUR);
    }

    fclose(index_file);
    fclose(bin_file);
}

void Schema::create_index_bplus(const std::string& bin_filename, const std::string& index_filename) const {
    FILE* bin_file = fopen(bin_filename.c_str(), "rb");
    bpt::bplus_tree bplus(index_filename.c_str(), true);

    int offset = 0;
    int pace = HEADER_SIZE + size - sizeof(int);
    int key;

    while(fread(&key, sizeof(int), 1, bin_file)) {
        bplus.insert(bpt::key_t(std::to_string(key).c_str()), offset);
        offset += pace;
    }

    fclose(bin_file);
}

void Schema:: create_index_direct_hash(const std::string& csv_filename, const std::string& bin_filename, bool ignore_first_line) const {
    std::ifstream csv_file(csv_filename);
    FILE* bin_file = fopen(bin_filename.c_str(), "wb");

    if(ignore_first_line) {
        std::string line;
        std::getline(csv_file, line);
    }

    int next_key = 0;

    std::size_t int_hash;
    std::hash<int> hash_fn;


    while(csv_file.good() && csv_file.peek() != EOF) {
        std::string timestamp = get_current_timestamp();

        // Compute hash value.
        int_hash = hash_fn(next_key);
	    fseek(bin_file, int_hash, SEEK_SET);
	
        // Write header.
        fwrite(&next_key, sizeof(int), 1, bin_file);
        fwrite(timestamp.c_str(), sizeof(char), TIMESTAMP_SIZE, bin_file);
        fwrite(&id, sizeof(int), 1, bin_file);
	
	    ++next_key;

        // Write data.
        for(unsigned i = 0; i < metadata.size(); ++i) {
            const auto& data = metadata[i];
            const char delimiter = (i == (metadata.size() - 1)) ? '\n' : ',';

            std::string token;
            std::getline(csv_file, token, delimiter);

            if (data.first[0] == 'i') {
                const int int_token = std::stoi(token);
                fwrite(&int_token, sizeof(int), 1, bin_file);
            }
            else {
                // ASSUMES: string.
                fwrite(token.c_str(), sizeof(char), atoi(data.first.c_str() + 1), bin_file);
            }
        }
    }

    fclose(bin_file);
    csv_file.close();
}

void Schema::create_index_indirect_hash(const std::string& bin_filename, const std::string& index_filename) const {

    FILE* bin_file = fopen(bin_filename.c_str(), "rb");
    FILE* index_file = fopen(index_filename.c_str(), "wb");

    int offset = 0;
    int pace = HEADER_SIZE + size - sizeof(int);
    int key;

    std::size_t int_hash;
    std::hash<int> hash_fn;

    while(fread(&key, sizeof(int), 1, bin_file)) {

	    // compute hash value
	    int_hash = hash_fn(key);
	
        fwrite(&int_hash, sizeof(size_t), 1, index_file);
        fwrite(&offset, sizeof(int), 1, index_file);

        offset += pace;
        fseek(bin_file, pace, SEEK_CUR);
    }

    fclose(index_file);
    fclose(bin_file);
}

void Schema::load_index(const std::string& index_filename) {
    index_map.clear();

    FILE* index_file = fopen(index_filename.c_str(), "rb");
    int key, offset;

    while(fread(&key, sizeof(int), 1, index_file)) {
        fread(&offset, sizeof(int), 1, index_file);
        index_map.push_back(std::make_pair(key, offset));
    }

    fclose(index_file);
}

void Schema::load_index_bplus(const std::string& index_filename) {
    bplus = new bpt::bplus_tree(index_filename.c_str());
}

void Schema::load_index_indirect_hash(const std::string& index_filename) {
	
    index_hash.clear();

    FILE* index_file = fopen(index_filename.c_str(), "rb");
    int key, offset;
    
    std::size_t int_hash;
    std::hash<int> hash_fn; 

    while(fread(&key, sizeof(int), 1, index_file)) {
        fread(&offset, sizeof(int), 1, index_file);
	    
        int_hash = hash_fn(key);
        index_hash.insert(std::make_pair(&int_hash, offset));
    }

    fclose(index_file);
}


void Schema::load_data(int pos, const std::string& bin_filename){
    FILE* bin_file = fopen(bin_filename.c_str(),"rb");
    pos += get_header_size();
    fseek(bin_file,pos,SEEK_SET);
    std::vector<char*> data;
    int string_size;

    for(unsigned i = 0; i < metadata.size() ; i++){
        string_size=atoi(metadata[i].first.c_str()+1);
        char* data_value=(char*)malloc(sizeof(char)*string_size);
        data.push_back(data_value);
        fread(data[i],sizeof(char),string_size,bin_file);
        std::cout<<data[i]<<std::endl;
        pos+=string_size;
    }
    fclose(bin_file);
}

std::vector<int> Schema::search_field(std::string field_name, std::string field_value, const std::string& bin_filename, int init_pos = 0) const{
    FILE* binfile = fopen(bin_filename.c_str(), "rb");
    
    int string_size;
    int pos = init_pos;
    int row_pos;
    int file_size;
    std::vector<int> pos_vec;

    fseek(binfile,0,SEEK_END);
    file_size = ftell(binfile);

    while(file_size > pos) {
        row_pos = pos;

        //Jump the header
        pos += get_header_size();
        fseek(binfile,pos,SEEK_SET);
        std::vector<char*> data;

        for(unsigned i = 0; i < metadata.size() ; i++){
            string_size=atoi(metadata[i].first.c_str()+1);
            if (metadata[i].second == field_name){
                char* data_value=(char*)malloc(sizeof(char)*string_size);
                data.push_back(data_value);
                fread(data[0],sizeof(char),string_size,binfile);
                if (data[0] == field_value){
                    //std::cout<<data[0]<<row_pos<<std::endl;
                    pos_vec.push_back(row_pos);
                }
            }
            pos += string_size;
            //fseek(binfile,pos,SEEK_SET);
        }
    }

    fclose(binfile);
    return pos_vec;
}

int Schema::search_for_key(int key) const {
    auto it = std::lower_bound(index_map.begin(), index_map.end(), std::make_pair(key, 0), [](const std::pair<int, int>& op1, const std::pair<int, int>& op2) {
        return op1.first < op2.first;
    });
    return it->second;
}

int Schema::search_for_key_bplus(int key) const {
    bpt::value_t value;
    // NOTE: if the return value is (-1), then such key hasn't been found.
    bplus->search(bpt::key_t(std::to_string(key).c_str()), &value);
    return value;
}

int Schema::search_for_key_indirect_hash(int key) const {

    // apply hash
    std::size_t int_hash; 
    std::hash<int> hash_fn; 
    int_hash = hash_fn(key);

    // find value in table
    std::unordered_map<std::size_t*,int>::const_iterator got = index_hash.find (&int_hash);
    return got->second; 
}

int Schema::search_for_key_direct_hash(int key, const std::string& bin_filename) const {

    FILE* binfile = fopen(bin_filename.c_str(), "rb");

    int k;

    //find record
    std::size_t int_hash=0;
    std::hash<int> hash_fn;
    fseek(binfile, hash_fn(key), SEEK_CUR); 
	
    if(fread(&k, sizeof(int), 1, binfile)){
    	if (k == key) {
     	 fclose(binfile);
      	 return int_hash;
    	}
    }
          
    fclose(binfile);
    return -1;
}

int Schema::search_for_key_raw(int key, const std::string& bin_filename) const {

    FILE* binfile = fopen(bin_filename.c_str(), "rb");

    int offset = 0;
    int pace = HEADER_SIZE + size - sizeof(int);
    int k;

    while(fread(&k, sizeof(int), 1, binfile)) {

        if (k == key) {
            fclose(binfile);
            return offset;
        }
        offset += pace;
        fseek(binfile, pace, SEEK_CUR);
    }

    fclose(binfile);
    return -1;
}
void Schema::join(Schema &schema2,Join_Conditions jc){
    switch(jc.type){
        case NATURAL_INNER:{
            join_natural_inner(schema2,jc);
            break;
        }
        case NATURAL_LEFT:{
            join_natural_left(schema2,jc);
            break;
        }
        case NATURAL_RIGHT:{
            join_natural_right(schema2,jc);
            break;
        }
        case NATURAL_FULL:{
            join_natural_full(schema2,jc);
        }
    }        
        
}

void Schema::join_natural_inner(Schema &schema2,Join_Conditions jc){
    switch(jc.implementation){
        case NESTED:{
            FILE* rel1 = fopen(jc.rel1_filename.c_str(), "rb");
            FILE* rel2 = fopen(jc.rel2_filename.c_str(), "rb");
                                            
            std::vector<int> pos_vec;

            fseek(rel1,0,SEEK_SET);
            fseek(rel2,0,SEEK_SET);

            while(!feof(rel1)) {
                while(!feof(rel2)){
                    /*row_pos = pos;

                    //Jump the header
                    pos += get_header_size();
                    fseek(binfile,pos,SEEK_SET);
                    std::vector<char*> data;

                    for(unsigned i = 0; i < metadata.size() ; i++){
                        string_size=atoi(metadata[i].first.c_str()+1);
                        if (metadata[i].second == field_name){
                            char* data_value=(char*)malloc(sizeof(char)*string_size);
                            data.push_back(data_value);
                            fread(data[0],sizeof(char),string_size,binfile);
                            if (data[0] == field_value){
                                //std::cout<<data[0]<<row_pos<<std::endl;
                                pos_vec.push_back(row_pos);
                            }
                        }
                        pos += string_size;
                        //fseek(binfile,pos,SEEK_SET);
                    } */
                }
            }
            fclose(rel1); 
            fclose(rel2);
            break;
        }
        case NESTED_EXISTING_INDEX:{
            break;
        }
        case NESTED_NEW_INDEX:{
            break;
        }
        case MERGE:{
            break;
        }
        case HASH:{
            break;
        }
        default:{
            break;
        }
    }
}

void Schema::join_natural_left(Schema &schema2,Join_Conditions jc){
    switch(jc.implementation){
        case NESTED:{            
            break;
        }
        case NESTED_EXISTING_INDEX:{
            break;
        }
        case NESTED_NEW_INDEX:{
            break;
        }
        case MERGE:{
            break;
        }
        case HASH:{
            break;
        }
        default:{
            break;
        }
    }
}

void Schema::join_natural_right(Schema &schema2,Join_Conditions jc){
    schema2.join_natural_left(*this,jc);
}

void Schema::join_natural_full(Schema &schema2,Join_Conditions jc){
    switch(jc.implementation){
        case NESTED:{            
            break;
        }
        case NESTED_EXISTING_INDEX:{
            break;
        }
        case NESTED_NEW_INDEX:{
            break;
        }
        case MERGE:{
            break;
        }
        case HASH:{
            break;
        }
        default:{
            break;
        }
    }
}

