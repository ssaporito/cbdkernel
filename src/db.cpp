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

int main(int argc, char *argv[]) {
  enum {
    OPERATION_CONVERT,
    OPERATION_CREATE_INDEX,
    OPERATION_CREATE_INDEX_BPLUS,
    OPERATION_LOAD_DATA,
    OPERATION_SEARCH_INDEX,
    OPERATION_SEARCH_INDEX_BPLUS,
    OPERATION_SEARCH_BENCHMARK,
    OPERATION_JOIN_EXISTING_INDEX,
    OPERATION_JOIN_NEW_INDEX,
    OPERATION_JOIN_MERGE,
    OPERATION_JOIN_HASH,
    OPERATION_JOIN_BENCHMARK,
    OPERATION_SEARCH_FIELD
  };

  int operation_flag = -1;

  static struct option long_options[] = {
    // Modes.
    {"convert", no_argument, &operation_flag, OPERATION_CONVERT},
    {"create-index", no_argument, &operation_flag, OPERATION_CREATE_INDEX},
    {"create-index-bplus", no_argument, &operation_flag, OPERATION_CREATE_INDEX_BPLUS},
    {"search-index", no_argument, &operation_flag, OPERATION_SEARCH_INDEX},
    {"search-index-bplus", no_argument, &operation_flag, OPERATION_SEARCH_INDEX_BPLUS},
    {"search-benchmark", no_argument, &operation_flag, OPERATION_SEARCH_BENCHMARK},
    {"search-field", no_argument, &operation_flag, OPERATION_SEARCH_FIELD},
    {"load-data", no_argument, &operation_flag, OPERATION_LOAD_DATA},
    {"join-existing-index", no_argument, &operation_flag, OPERATION_JOIN_EXISTING_INDEX},
    {"join-new-index", no_argument, &operation_flag, OPERATION_JOIN_NEW_INDEX},
    {"join-merge", no_argument, &operation_flag, OPERATION_JOIN_MERGE},
    {"join-hash", no_argument, &operation_flag, OPERATION_JOIN_HASH},

    // Mode options.
    {"schema", required_argument, NULL, 0},
    {"schemadb", required_argument, NULL, 0},
    {"schemadb2", optional_argument, NULL, 0},
    {"in", required_argument, NULL, 'i'},
    {"out", required_argument, NULL, 'o'},
    {"key", required_argument, NULL, 0},
    {"pos",required_argument,NULL,0},
    {"field_name", optional_argument, NULL, 0},
    {"field_value", optional_argument, NULL, 0},
    {"indexfile", optional_argument, NULL, 0},
    {"bplusfile", optional_argument, NULL, 0},


    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0},
  };

  int option_index = 0;
  int ch;

  std::string schemadb_filename;
  std::string schemadb_filename2;
  int schema_id = 0;
  int schema_id2 = 0;
  int key = 0;
  int pos=0;
  std::string field_name, field_value;
  std::string infile, outfile;
  std::string indexfile, bplusfile;

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
        if(!strcmp(long_options[option_index].name, "schemadb2")) {          
          schemadb_filename2 = std::string(optarg);
        }
        else if(!strcmp(long_options[option_index].name, "schema")) {
          schema_id = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "pos")) {
          pos = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "schema2")) {
          schema_id2 = std::stoi(std::string(optarg));
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
        else if(!strcmp(long_options[option_index].name, "bplusfile")) {
          bplusfile = std::string(optarg);
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
  SchemaDb schemadb2(schemadb_filename2);
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
      for(int i=0;i<10;i++){
        schema.load_data(pos,infile);
        pos+=schema.get_size();     
      }
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
    case OPERATION_SEARCH_FIELD:
      std::cout << "mode: search field" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.search_field(field_name, field_value, infile);
      break;
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
    case OPERATION_JOIN_EXISTING_INDEX:
      {
      std::cout << "mode: join with existing index" << std::endl;
      schema1 = schemadb.get_schema(schema_id);
      schema2 = schemadb2.get_schema(schema_id2);
      schema1.load_index(infile);
      schema1.join_existing_index(schema2);
      break;
      }
    case OPERATION_JOIN_NEW_INDEX:
      {
      std::cout << "mode: join with new index" << std::endl;
      schema1 = schemadb.get_schema(schema_id);
      schema2 = schemadb2.get_schema(schema_id2);
      schema1.join_new_index(schema2);
      break;
      }
    case OPERATION_JOIN_MERGE:
      {
      std::cout << "mode: join through merge" << std::endl;
      schema1 = schemadb.get_schema(schema_id);
      schema2 = schemadb2.get_schema(schema_id2);
      schema1.join_merge(schema2);
      break;
      }
    case OPERATION_JOIN_HASH:
      {
      std::cout << "mode: join through hash" << std::endl;
      schema1 = schemadb.get_schema(schema_id);
      schema2 = schemadb2.get_schema(schema_id2);
      schema1.join_hash(schema2);
      break;
      }
  }


  return EXIT_SUCCESS;
}

