#include <iostream>
#include <string>
#include <vector>
#include <fstream>

int main(int argc, char * argv []) {

    // check usage
    if (argc != 4) {

        std::cout << "Incorrect program usage!" << std::endl;
        return 1;

    }

    // get file names
    std::ifstream names_file (argv[1]);
    std::ifstream last_file (argv[2]);
    std::ofstream out_file (argv [3]);

    int elements = 10000000;
    
    std::vector<std::string> names;
    std::vector<std::string> last_names;


    // load structures
    while (names_file.good() && names_file.peek() != EOF){

        std::string line;
        std::getline(names_file, line);
        names.push_back(line);

    }


    while (last_file.good() && last_file.peek() != EOF){

        std::string line;
        std::getline(last_file, line);
        last_names.push_back(line);

    }

    out_file << "name" << "," << "last name" << std::endl;
    // gen records
    int key = 0;
    for (const auto& name: names) {
        for (const auto& last_name: last_names) {

	  out_file << name << "," << last_name << std::endl;
            ++ key;
	    if (key == elements) break;

        }
    }


    std::cout << " Number of entries generated: " << key << std::endl;

    // close streams
    names_file.close();
    last_file.close();
    out_file.close();

    return 0;
}
