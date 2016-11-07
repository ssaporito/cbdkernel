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
    int i=0;
    int offset=0; 
    while(input.good() && input.peek() != EOF) {
        std::string datatype, column;
        std::getline(input, datatype, ',');
        std::getline(input, column);
        
        
        metadata.push_back(std::make_pair(datatype, column));
        column_index.insert(std::make_pair(column,i));
        column_offset.insert(std::make_pair(column,offset));
        int column_size=0;
        if(datatype=="int"){
            column_size=sizeof(int);
        }
        else{
            column_size=atoi(datatype.c_str()+1);  // Assumes string   
        }
        offset+=column_size;
        i++;
    }

    input.close();

    compute_size();
    compute_header_size();
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

int Schema::get_data_size() const {
    return size;
}

int Schema::get_header_size() const {
    return header_size;
}

int Schema::get_row_size() const{
    return header_size+size;
}
std::vector< std::pair<std::string, std::string> > Schema::get_metadata() const{
    return metadata;
}

std::unordered_map<std::string, int> Schema::get_column_index() const{
    return column_index;
}

std::unordered_map<std::string, int> Schema::get_column_offset() const{
    return column_offset;
}

std::vector<std::pair<int, int> > Schema::get_index_map() const{
    return index_map;
}
std::unordered_map<std::size_t*,int> Schema::get_index_hash() const{
    return index_hash;
}

std::string Schema::get_filename() const {
    return schema_filename;
}

std::vector<std::string> Schema::get_table(const std::string& rel_filename,const std::string& field_name) const{
    int offset=column_offset.at(field_name);    
    int index=column_index.at(field_name);    

    FILE* rel = fopen(rel_filename.c_str(), "rb");
    fseek(rel,0,SEEK_END);
    int rel_size = ftell(rel);        
    int pos=0; 
    fseek(rel,0,SEEK_SET); 
    std::vector<std::string> data;                
    while(pos<rel_size) {        
        pos+=get_header_size();
        fseek(rel,pos+offset,SEEK_SET);
        int column_size=atoi(metadata[index].first.c_str()+1);            
        char* value=(char*)malloc(sizeof(char)*column_size);               
        fread(value,sizeof(char),column_size,rel); 
        //std::cout<<rel_filename<<" "<<value<<std::endl;       
        data.push_back(value);
        pos+=get_data_size();
    }
    fclose(rel);
    return data;
}

std::unordered_map<std::string,std::vector<int>> Schema::get_table_map(const std::string& rel_filename,const std::string& field_name) const{
    int offset=column_offset.at(field_name);    
    int index=column_index.at(field_name);    

    FILE* rel = fopen(rel_filename.c_str(), "rb");
    fseek(rel,0,SEEK_END);
    int rel_size = ftell(rel);        
    int pos=0; 
    fseek(rel,0,SEEK_SET); 
    std::unordered_map<std::string,std::vector<int>> data;    
    unsigned i=0;
    while(pos<rel_size) { 
        pos+=get_header_size();               
        fseek(rel,pos+offset,SEEK_SET);
        int column_size=atoi(metadata[index].first.c_str()+1);            
        char* value=(char*)malloc(sizeof(char)*column_size);               
        fread(value,sizeof(char),column_size,rel); 
        //std::cout<<rel_filename<<" "<<value<<std::endl; 
        if(data.find(value)!=data.end()){
            data[value].push_back(i);
        }   
        else{
            std::vector<int> indexes(1,i);
            data.insert(std::make_pair(value,indexes));            
        }               
        i++;
        pos+=get_data_size();
    }    
    fclose(rel);
    return data;
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
                //std::cout<<int_token<<std::endl;
            }
            else {
                // ASSUMES: string.
                fwrite(token.c_str(), sizeof(char), atoi(data.first.c_str() + 1), bin_file);
                //std::cout<<token.c_str()<<std::endl;
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
                size_t string_size=atoi(metadata[i].first.c_str()+1);
                char* data_value=(char*)malloc(sizeof(char)*string_size);
                fread(data_value,sizeof(char),string_size,bin_file);  
                std::cout<<data_value;  
            }  
            if(i==metadata.size()-1){
                std::cout<<std::endl;
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
    if(pos!=-1){
        FILE* bin_file = fopen(bin_filename.c_str(),"rb");
        pos += get_header_size();
        fseek(bin_file,pos,SEEK_SET);    
        int string_size;

        for(unsigned i = 0; i < metadata.size() ; i++){
            string_size=atoi(metadata[i].first.c_str()+1);
            char* data_value=(char*)malloc(sizeof(char)*string_size);        
            fread(data_value,sizeof(char),string_size,bin_file);
            std::cout<<data_value<<((i==metadata.size()-1)?(""):(","));//<<column_offset[metadata[i].second]<<std::endl;
            pos+=string_size;
        }
        fclose(bin_file);
    }
    else{ // print null columns for pos=-1 (used in joins)
        for(unsigned i = 0; i < metadata.size() ; i++){            
            std::cout<<"NULL"<<((i==metadata.size()-1)?(""):(","));//<<column_offset[metadata[i].second]<<std::endl;            
        }
    }
}

std::vector<int> Schema::search_field(std::string field_name, std::string field_value, const std::string& bin_filename, int init_pos = 0) const{
    std::vector<int> pos_vec;
    if(column_offset.find(field_name)!=column_offset.end()){
        int offset=column_offset.at(field_name);
        int index=column_index.at(field_name);
        FILE* binfile = fopen(bin_filename.c_str(), "rb");    
        int string_size;
        int pos = init_pos;
        int row_pos;
        int file_size;        

        fseek(binfile,0,SEEK_END);
        file_size = ftell(binfile);

        while(file_size > pos) {
            row_pos = pos;

            //Jump the header
            pos += get_header_size();
            fseek(binfile,pos+offset,SEEK_SET);                    
            string_size=atoi(metadata[index].first.c_str()+1);            
            char* data_value=(char*)malloc(sizeof(char)*string_size);                
            fread(data_value,sizeof(char),string_size,binfile);
            if (data_value == field_value){
                //std::cout<<data[0]<<row_pos<<std::endl;
                pos_vec.push_back(row_pos);
            }                        
            pos+=get_data_size();
        }

        fclose(binfile);
    }
    else{
        std::cout<<"Column not in table schema."<<std::endl;
    }
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
    std::vector<std::pair<int,int>> pos_vector;
    switch(jc.type){
        case NATURAL_INNER:{
            pos_vector=join_natural_inner(schema2,jc);
            break;
        }
        case NATURAL_LEFT:{
            pos_vector=join_natural_left(schema2,jc);
            break;
        }
        case NATURAL_RIGHT:{
            pos_vector=join_natural_right(schema2,jc);
            break;
        }
        case NATURAL_FULL:{
            pos_vector=join_natural_full(schema2,jc);
        }
    }   
    for(unsigned i=0;i<metadata.size();i++){
        std::cout<<metadata[i].second<<",";
    }
    std::vector<std::pair<std::string, std::string>> metadata2=schema2.get_metadata();
    for(unsigned j=0;j<metadata2.size();j++){
        std::cout<<metadata2[j].second<<((j==metadata2.size()-1)?(""):(","));   
    }
    std::cout<<std::endl;   
    for(unsigned i=0;i<pos_vector.size();i++){
        load_data(pos_vector[i].first,jc.rel1_filename);
        std::cout<<",";
        schema2.load_data(pos_vector[i].second,jc.rel2_filename);
        std::cout<<std::endl;
    }    
}

std::vector<std::pair<int,int>> Schema::join_natural_inner(Schema &schema2,Join_Conditions jc){
    std::vector<std::pair<int,int>> pos_vector_left,pos_vector_right,pos_vector;    
    pos_vector_left=join_natural_left(schema2,jc);
    pos_vector_right=join_natural_right(schema2,jc);
    for(unsigned i=0;i<pos_vector_left.size();i++){        
        for(unsigned j=0;j<pos_vector_right.size();j++){
            if(pos_vector_left[i]==pos_vector_right[j]){
                pos_vector.push_back(pos_vector_right[j]);
            }
        }        
    }
    return pos_vector;
}

std::vector<std::pair<int,int>> Schema::join_natural_left(Schema &schema2,Join_Conditions jc){
    std::vector<std::pair<int,int>> pos_vector;
    switch(jc.implementation){
        case NESTED:{  
            FILE* rel1 = fopen(jc.rel1_filename.c_str(), "rb");
            FILE* rel2 = fopen(jc.rel2_filename.c_str(), "rb");
                                            
            std::vector<int> pos_vec;
                        
            int offset1=column_offset.at(jc.field_name);
            int offset2=schema2.get_column_offset().at(jc.field_name);

            int index1=column_index.at(jc.field_name);
            int index2=schema2.get_column_index().at(jc.field_name);
                       

            fseek(rel1,0,SEEK_END);
            int rel_size1 = ftell(rel1);

            fseek(rel2,0,SEEK_END);
            int rel_size2 = ftell(rel2);

            int pos1=0; 
            fseek(rel1,0,SEEK_SET);            
            while(pos1<rel_size1) {
                int row_pos1=pos1;
                pos1+=get_header_size();
                fseek(rel1,pos1+offset1,SEEK_SET);
                int column_size1=atoi(metadata[index1].first.c_str()+1);            
                char* value1=(char*)malloc(sizeof(char)*column_size1);                
                fread(value1,sizeof(char),column_size1,rel1);

                int pos2=0;
                fseek(rel2,0,SEEK_SET);
                bool found_joinable=false;
                while(pos2<rel_size2){
                    int row_pos2=pos2;
                    pos2+=schema2.get_header_size();
                    fseek(rel2,pos2+offset2,SEEK_SET);
                    int column_size2=atoi(schema2.get_metadata()[index2].first.c_str()+1);            
                    char* value2=(char*)malloc(sizeof(char)*column_size2);                
                    fread(value2,sizeof(char),column_size2,rel2);
                    if(!strcmp(value1,value2)){
                        found_joinable=true;
                        //std::cout<<"Joined "<<value1<<" at positions "<<row_pos1<<","<<row_pos2<<std::endl;
                        pos_vector.push_back(std::make_pair(row_pos1,row_pos2));
                    }
                    //std::cout<<value1<<" "<<value2<<std::endl;
                    pos2+=schema2.get_data_size();
                }      
                if(!found_joinable){
                    pos_vector.push_back(std::make_pair(row_pos1,-1));
                }          
                pos1+=get_data_size();
            }
            fclose(rel1); 
            fclose(rel2);          
            break;
        }
        case NESTED_EXISTING_INDEX:{

            FILE* rel1 = fopen(jc.rel1_filename.c_str(), "rb");
            FILE* rel2 = fopen(jc.rel2_filename.c_str(), "rb");

            int offset1=column_offset.at(jc.field_name);
            int offset2=schema2.get_column_offset().at(jc.field_name);

            int index1=column_index.at(jc.field_name);
            int index2=schema2.get_column_index().at(jc.field_name);
                       
                                            
            std::vector<int> pos_vec;

            for(unsigned i = 0 ; i < index_map.size() ; i++){
               
                int pos1=index_map[i].second+i*4;
                int row_pos1 = pos1;
                pos1+=get_header_size();
                fseek(rel1,pos1+offset1,SEEK_SET);
                int column_size1=atoi(metadata[index1].first.c_str()+1);            
                char* value1=(char*)malloc(sizeof(char)*column_size1);                
                fread(value1,sizeof(char),column_size1,rel1);
               
                for(unsigned j = 0 ; j < schema2.get_index_map().size() ; j++){

                    int pos2 = schema2.get_index_map()[j].second+i*4;
                    int row_pos2=pos2;
                    pos2+=schema2.get_header_size();
                    fseek(rel2,pos2+offset2,SEEK_SET);
                    int column_size2=atoi(schema2.get_metadata()[index2].first.c_str()+1);            
                    char* value2=(char*)malloc(sizeof(char)*column_size2);                
                    fread(value2,sizeof(char),column_size2,rel2);
                    
                    if(!strcmp(value1,value2)){
                        //std::cout<<"Joined "<<value1<<" at positions "<<row_pos1<<","<<row_pos2<<std::endl;
                        pos_vector.push_back(std::make_pair(row_pos1,row_pos2));
                    }
                }
            } 

            fclose(rel1); 
            fclose(rel2);
            break;
        }
        case NESTED_NEW_INDEX:{
            FILE* rel1 = fopen(jc.rel1_filename.c_str(), "rb");
            FILE* rel2 = fopen(jc.rel2_filename.c_str(), "rb");
            FILE* ind1 = fopen("../data/csv/schema_test1.index","wb");
            FILE* ind2 = fopen("../data/csv/schema_test2.index","wb");
            bool indexload = false;
            std::vector<std::pair<char*,int>> index_map1;
            std::vector<std::pair<char*,int>> index_map2;


            std::vector<int> pos_vec;
                        
            int offset1=column_offset.at(jc.field_name);
            int offset2=schema2.get_column_offset().at(jc.field_name);

            int index1=column_index.at(jc.field_name);
            int index2=schema2.get_column_index().at(jc.field_name);
                       

            fseek(rel1,0,SEEK_END);
            int rel_size1 = ftell(rel1);

            fseek(rel2,0,SEEK_END);
            int rel_size2 = ftell(rel2);

            int pos1=0; 
            fseek(rel1,0,SEEK_SET);            
            while(pos1<rel_size1) {
                int row_pos1=pos1;
                pos1+=get_header_size();
                fseek(rel1,pos1+offset1,SEEK_SET);
                int column_size1=atoi(metadata[index1].first.c_str()+1);            
                char* value1=(char*)malloc(sizeof(char)*column_size1);                
                fread(value1,sizeof(char),column_size1,rel1);
                fwrite(value1,sizeof(char),column_size1,ind1);
                fwrite(&row_pos1,sizeof(int),1,ind1);
                index_map1.push_back(std::make_pair(value1,row_pos1));
                int pos2=0;
                fseek(rel2,0,SEEK_SET);
                bool found_joinable=false;
                if(!indexload){
                    while(pos2<rel_size2){

                        int row_pos2=pos2;
                        pos2+=schema2.get_header_size();
                        fseek(rel2,pos2+offset2,SEEK_SET);
                        int column_size2=atoi(schema2.get_metadata()[index2].first.c_str()+1);            
                        char* value2=(char*)malloc(sizeof(char)*column_size2);                
                        fread(value2,sizeof(char),column_size2,rel2);
                        
                        fwrite(value2,sizeof(char),column_size2,ind2);
                        fwrite(&row_pos2,sizeof(int),1,ind2);

                        index_map2.push_back(std::make_pair(value2,row_pos2));

                        if(!strcmp(value1,value2)){
                            found_joinable=true;
                            //std::cout<<"Joined "<<value1<<" at positions "<<row_pos1<<","<<row_pos2<<std::endl;
                            pos_vector.push_back(std::make_pair(row_pos1,row_pos2));
                        }
                        //std::cout<<value1<<" "<<value2<<std::endl;
                        pos2+=schema2.get_data_size();
                    }  
                }else{
                    for(unsigned i=0 ;i < index_map2.size() ; i++){
                        if(!strcmp(value1,index_map2[i].first)){
                            found_joinable=true;
                            //std::cout<<"Joined "<<value1<<" at positions "<<row_pos1<<","<<row_pos2<<std::endl;
                            pos_vector.push_back(std::make_pair(row_pos1,index_map2[i].second));
                        }
                    }

                }   

                if(!found_joinable){
                    pos_vector.push_back(std::make_pair(row_pos1,-1));
                }          
                pos1+=get_data_size();

                indexload=true;
            }

            fclose(rel1); 
            fclose(ind1);
            fclose(rel2);
            fclose(ind2);          

            break;
        }
        case MERGE:{                                                                                  
            std::vector<std::string> data1,data2;
            data1=get_table(jc.rel1_filename,jc.field_name);
            data2=schema2.get_table(jc.rel2_filename,jc.field_name);

            // keep track of old indexes
            auto mapped_indexes1=sort_indexes(data1); 
            auto mapped_indexes2=sort_indexes(data2);

            // sort alphabetically
            std::sort(data1.begin(),data1.end());
            std::sort(data2.begin(),data2.end());

            unsigned j_start=0;            
            for(unsigned i=0;i<data1.size();i++){
                bool found_joinable=false;
                for(unsigned j=j_start;j<data2.size();j++){                                   
                    //std::cout<<data1[i]<<" "<<data2[j]<<" "<<i<<" "<<j<<std::endl;
                    if(data1[i]==data2[j]){
                        found_joinable=true;                            
                        pos_vector.push_back(std::make_pair(mapped_indexes1[i]*(get_row_size()),mapped_indexes2[j]*(schema2.get_row_size())));
                        j_start=j;
                    //    std::cout<<data1[i]<<" "<<data2[j]<<" "<<i<<" "<<j<<std::endl;
                    }
                    else if(data1[i]<data2[j]){                                            
                        break;  // skip to next i if key2>key1
                    }                                
                }
                if(!found_joinable){
                    pos_vector.push_back(std::make_pair(mapped_indexes1[i]*(get_row_size()),-1));
                }
            }
                      
            break;
        }
        case HASH:{
            std::vector<std::string> data1;          
            std::unordered_map<std::string, std::vector<int>> data2;             
            data1=this->get_table(jc.rel1_filename,jc.field_name); // vector
            data2=schema2.get_table_map(jc.rel2_filename,jc.field_name); // hashmap
            /*for(auto k:data2){
                std::cout<<k.first<<":";
                for(auto j:k.second){
                    std::cout<<j<<",";
                } 
                std::cout<<std::endl;       
            }*/
            /*for(unsigned i=0;i<data1.size();i++){
                std::cout<<data1[i]<<std::endl;
            }*/
            for(unsigned i=0;i<data1.size();i++){
                if(data2.find(data1[i])!=data2.end()){
                    for(auto idx:data2[data1[i]]){
                        pos_vector.push_back(std::make_pair(i*(get_row_size()),idx*schema2.get_row_size()));
                    }                    
                }
                else{
                    pos_vector.push_back(std::make_pair(i*(get_row_size()),-1));
                }
            }            
            break;
        }
        default:{
            break;
        }
    }
    return pos_vector;
}

std::vector<std::pair<int,int>> Schema::join_natural_right(Schema &schema2,Join_Conditions jc){
    std::vector<std::pair<int,int>> pos_vector;
    Join_Conditions jc2;
    jc2=jc;
    jc2.rel2_filename=jc.rel1_filename;
    jc2.rel1_filename=jc.rel2_filename;
    pos_vector=schema2.join_natural_left(*this,jc2);
    for(unsigned i=0;i<pos_vector.size();i++){
        pos_vector[i]=std::make_pair(pos_vector[i].second,pos_vector[i].first);
    }
    return pos_vector;
}

std::vector<std::pair<int,int>> Schema::join_natural_full(Schema &schema2,Join_Conditions jc){
    std::vector<std::pair<int,int>> pos_vector_left,pos_vector_right,pos_vector;    
    pos_vector_left=join_natural_left(schema2,jc);
    pos_vector_right=join_natural_right(schema2,jc);
    for(unsigned i=0;i<pos_vector_left.size();i++){
        pos_vector.push_back(pos_vector_left[i]);
        for(unsigned j=0;j<pos_vector_right.size();j++){
            if(pos_vector_left[i]!=pos_vector_right[j]){
                pos_vector.push_back(pos_vector_right[j]);
            }
        }        
    }
    return pos_vector;
}