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
    OPERATION_SEARCH_INDEX,
    OPERATION_SEARCH_INDEX_BPLUS,
    OPERATION_SEARCH_BENCHMARK
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

    // Mode options.
    {"schema", required_argument, NULL, 0},
    {"schemadb", required_argument, NULL, 0},
    {"in", required_argument, NULL, 'i'},
    {"out", required_argument, NULL, 'o'},
    {"key", required_argument, NULL, 0},
    {"indexfile", optional_argument, NULL, 0},
    {"bplusfile", optional_argument, NULL, 0},


    {"help", no_argument, NULL, 'h'},
    {NULL, 0, NULL, 0},
  };

  int option_index = 0;
  int ch;

  std::string schemadb_filename;
  int schema_id = 0;
  int key = 0;
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
        else if(!strcmp(long_options[option_index].name, "schema")) {
          schema_id = std::stoi(std::string(optarg));
        }
        else if(!strcmp(long_options[option_index].name, "key")) {
          key = std::stoi(std::string(optarg));
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
  Schema schema;

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
    case OPERATION_SEARCH_INDEX:
      std::cout << "mode: search index" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.load_index(infile);
      schema.search_for_key(key);
      break;
    case OPERATION_SEARCH_INDEX_BPLUS:
      std::cout << "mode: search index w/ B+ tree" << std::endl;
      schema = schemadb.get_schema(schema_id);
      schema.load_index_bplus(infile);
      schema.search_for_key_bplus(key);
      break;
    case OPERATION_SEARCH_BENCHMARK:    
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

  return EXIT_SUCCESS;
}
