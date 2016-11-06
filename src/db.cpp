#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include <iostream>
#include <string>
#include <time.h>
#include <random>

#include "schemadb.hpp"
#include "benchmark.hpp"

void usage(const char* program) {
  std::cout << "usage: " << program << " --schemadb=<schemadb_filename> --schema=<schema_id> <mode> <mode options>" << std::endl;
  std::cout << "\t" << "mode: --convert --in <.csv file> --out <.bin file>" << std::endl;
  std::cout << "\t" << "mode: --create-index --in <.bin file> --out <.index file>" << std::endl;
  std::cout << "\t" << "mode: --create-index-bplus --in <.bin file> --out <.index file>" << std::endl;
  std::cout << "\t" << "mode: --search-index --in <.index file> --key <key>" << std::endl;
  std::cout << "\t" << "mode: --search-index-bplus --in <.index file> --key <key>" << std::endl;

  exit(EXIT_FAILURE);
}

#define BENCHMARK(f) \
do { \
  clock_t start = clock(); \
  f; \
  clock_t end = clock(); \
  int usec = ((end - start) * 1e6) / CLOCKS_PER_SEC; \
  printf("Time taken %d milliseconds %d microseconds\n", usec / 1000, usec % 1000); \
} while(false)

join_implementation string_to_join_implementation(std::string string_join_impl){
    if(string_join_impl=="nested"){
        return NESTED;
    }
    else if(string_join_impl=="nested_existing_index"){
        return NESTED_EXISTING_INDEX;
    }
    else if(string_join_impl=="nested_new_index"){
        return NESTED_NEW_INDEX;
    }
    else if(string_join_impl=="merge"){
        return MERGE;
    }
    else if(string_join_impl=="hash"){
        return HASH;
    }
    else return NESTED;
}

join_type string_to_join_type(std::string string_join_type){
    if(string_join_type=="natural_inner"){
        return NATURAL_INNER;
    }
    else if(string_join_type=="natural_left"){
        return NATURAL_LEFT;
    }
    else if(string_join_type=="natural_right"){
        return NATURAL_RIGHT;
    }
    else if(string_join_type=="natural_full"){
        return NATURAL_FULL;
    }    
    else return NATURAL_INNER;
}

int main(int argc, char *argv[]) {
  enum {
    OPERATION_CONVERT,
    OPERATION_PRINT_BIN,
    OPERATION_CREATE_INDEX,
    OPERATION_CREATE_INDEX_BPLUS,
    OPERATION_LOAD_DATA,
    OPERATION_SEARCH_INDEX,
    OPERATION_SEARCH_INDEX_BPLUS,
    OPERATION_SEARCH_BENCHMARK,
    OPERATION_JOIN,
    OPERATION_JOIN_BENCHMARK,
    OPERATION_SEARCH_FIELD
  };

  int operation_flag = -1;

  static struct option long_options[] = {
    // Modes.
    {"convert", no_argument, &operation_flag, OPERATION_CONVERT},
    {"print-bin", no_argument, &operation_flag, OPERATION_PRINT_BIN},
    {"create-index", no_argument, &operation_flag, OPERATION_CREATE_INDEX},
    {"create-index-bplus", no_argument, &operation_flag, OPERATION_CREATE_INDEX_BPLUS},
    {"search-index", no_argument, &operation_flag, OPERATION_SEARCH_INDEX},
    {"search-index-bplus", no_argument, &operation_flag, OPERATION_SEARCH_INDEX_BPLUS},
    {"search-benchmark", no_argument, &operation_flag, OPERATION_SEARCH_BENCHMARK},
    {"search-field", no_argument, &operation_flag, OPERATION_SEARCH_FIELD},
    {"load-data", no_argument, &operation_flag, OPERATION_LOAD_DATA},
    {"join", no_argument, &operation_flag, OPERATION_JOIN},

    // Mode options.
    {"schema", required_argument, NULL, 0},
    {"schema2", required_argument, NULL, 0},
    {"schemadb", required_argument, NULL, 0},
    {"in", required_argument, NULL, 'i'},
    {"in2", required_argument, NULL, 0},
    {"join-type", optional_argument, NULL, 0},
    {"join-impl", optional_argument, NULL, 0},    
    {"out", required_argument, NULL, 'o'},
    {"key", required_argument, NULL, 0},
    {"pos",required_argument,NULL,0},
    {"init_pos", optional_argument,NULL, 0},
    {"field_name", optional_argument, NULL, 0},
    {"field_value", optional_argument, NULL, 0},
    {"indexfile", optional_argument, NULL, 0},
    {"indexfile2", optional_argument, NULL, 0},
    {"bplusfile", optional_argument, NULL, 0},


    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0},
  };

  int option_index = 0;
  int ch;

  std::string schemadb_filename;  
  int schema_id = 0;
  int schema_id2 = 0;
  int key = 0;
  int pos=0;
  int init_pos = 0;
  std::string field_name, field_value;
  std::string infile,infile2, outfile;
  std::string indexfile, indexfile2, bplusfile;
  join_implementation join_impl;
  join_type join_tp;

  while((ch = getopt_long(argc, argv, "hi:o:", long_options, &option_index)) != -1) {
    switch(ch) {
      case 'i':
        infile = std::string(optarg);
        break;
      case 'o':
        outfile = std::string(optarg);
        break;
      case 0:
        if(!strcmp(long_options[option_index].name, "schemadb")) {
          schemadb_filename = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "schema")) {
          schema_id = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "schema2")) {
          schema_id2 = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "in2")) {
          infile2 = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "pos")) {
          pos = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "init_pos")) {
          init_pos = std::stoi(std::string(optarg));
        }        
        else if(!strcmp(long_options[option_index].name, "key")) {
          key = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "field_name")) {
          field_name = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "field_value")) {
          field_value = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "indexfile")) {
          indexfile = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "indexfile2")) {
          indexfile2 = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "bplusfile")) {
          bplusfile = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "join-impl")) {
          join_impl = string_to_join_implementation(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "join-type")) {
          join_tp = string_to_join_type(std::string(optarg));
        }
        break;
      case 'h':
      case '?':
      case ':':
      default:
        usage(argv[0]);
    }
  }

  // Validate arguments.
  if(schemadb_filename.empty()) {
    std::cout << "error: schemadb_filename not specified" << std::endl;
    usage(argv[0]);
  }

  SchemaDb schemadb(schemadb_filename);  
  Schema schema,schema1,schema2;


  switch(operation_flag) {
    case -1:
      std::cout << "error: no mode specified" << std::endl;
      usage(argv[0]);
      break;
    case OPERATION_CONVERT:
      std::cout << "mode: convert" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.convert_to_bin(infile, outfile, true);
      break;
    case OPERATION_PRINT_BIN:
      std::cout << "mode: print bin" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.print_binary(infile);
      break;
    case OPERATION_CREATE_INDEX:
      std::cout << "mode: create index" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.create_index(infile, outfile);
      break;
    case OPERATION_CREATE_INDEX_BPLUS:
      std::cout << "mode: create index w/ B+ tree" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.create_index_bplus(infile, outfile);
      break;
    case OPERATION_LOAD_DATA:
      std::cout << "mode: load data" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.load_data(pos,infile);
      /*for(int i=0;i<100;i++){
        schema.load_data(pos,infile);
        pos+=schema.get_header_size()+schema.get_size();     
      }*/
      //schema.load_data(pos, infile);
      break;
    case OPERATION_SEARCH_INDEX:
      std::cout << "mode: search index" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.load_index(infile);
      int aux;
      aux = schema.search_for_key(key);
      std::cout<<aux<<std::endl;
      break;
    case OPERATION_SEARCH_INDEX_BPLUS:
      std::cout << "mode: search index w/ B+ tree" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.load_index_bplus(infile);
      schema.search_for_key_bplus(key);
      break;
    case OPERATION_SEARCH_FIELD:{
      std::cout << "mode: search field" << std::endl;
      schema = schemadb.get_schema(schema_id);
      std::vector<int> row_vec = schema.search_field(field_name, field_value, infile, init_pos);
      for (unsigned i=0; i<row_vec.size(); i++){
        schema.load_data(row_vec[i],infile);
      }
      break;}
    case OPERATION_SEARCH_BENCHMARK:    
      {
      std::cout << "mode: search methods benchmarking" << std::endl;
      
      std::string schemabin ("../data/schema/company.bin");
      std::string index ("../data/schema/company.index");
      std::string bindex ("../data/schema/company.bindex");
      schema = schemadb.get_schema(schema_id);
      
      std::cout << "converting to bin" << std::endl;

      // create files
      schema.convert_to_bin(infile, schemabin);

      std::cout << "creating index" << std::endl;

      schema.create_index(schemabin, index);

      std::cout << "creating bplus index" << std::endl;

      schema.create_index_bplus(schemabin, bindex);

      std::cout << "indexes have been created" << std::endl;

      schema.load_index(index);
      schema.load_index_bplus(bindex);

      std::cout << "indexes have been loaded" << std::endl;

      // gen search keys
      std::default_random_engine rgen;
      std::uniform_int_distribution<int> distribution(0,9808);
      int key = distribution(rgen);

      int lkey = 5000;
      int hkey = 7000;

      std::vector<int> randomset;
      for (unsigned i = 0; i < 100; ++i) randomset.push_back(distribution(rgen));

      std::cout << "random set has been generated" << std::endl;

      // sigle key search
      std::cout << "Single search" << std::endl << std::endl;;
      std::cout << "Sequential Index" << std::endl;
      BENCHMARK(schema.search_for_key(key));
      std::cout << "BPlus" << std::endl;
      BENCHMARK(schema.search_for_key_bplus(key));
      std::cout << "Raw file brute force" << std::endl;
      BENCHMARK(schema.search_for_key_raw(key, "../data/schema/company.bin"));
      std::cout << std::endl;

      // set search
      std::cout << "Random set search" << std::endl << std::endl;;
      std::cout << "Sequential Index" << std::endl; 
      BENCHMARK(search_set(schema, randomset));
      std::cout << "BPlus" << std::endl;
      BENCHMARK(search_set_bplus(schema, randomset));
      std::cout << "Raw file brute force" << std::endl;
      BENCHMARK(search_set_raw(schema, randomset, "../data/schema/company.bin"));
      std::cout << std::endl;

      // range search
      std::cout << "Range search" << std::endl << std::endl;
      BENCHMARK(search_range(schema, lkey, hkey));
      std::cout << "BPlus" << std::endl;
      BENCHMARK(search_range_bplus(schema, lkey, hkey));
      std::cout << "Raw file brute force" << std::endl;
      BENCHMARK(search_range_raw(schema, lkey, hkey, "../data/schema/company.bin"));
      std::cout << std::endl;      


      break;
      }
    case OPERATION_JOIN:
      {
      std::cout << "mode: join" << std::endl;
      schema1 = schemadb.get_schema(schema_id);
      schema2 = schemadb.get_schema(schema_id2);  
      Join_Conditions jc;
      jc.rel1_filename=infile.c_str();
      jc.rel2_filename=infile2.c_str();
      jc.field_name=field_name;
      jc.type=join_tp;
      jc.implementation=join_impl;
      if(jc.implementation == NESTED_EXISTING_INDEX){
        
        schema1.load_index(indexfile);
        schema2.load_index(indexfile2);
      }   

      schema1.join(schema2,jc);

      break;
      }
  }


  return EXIT_SUCCESS;
}

