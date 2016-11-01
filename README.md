# cbdkernel

Compilar:

make

Executar com benchmark:

./db --search-benchmark --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.csv

./db --search-field --field_name="nome" --field_value="valor" --schemadb=schemadb_path --schema id --in bin_file

1. TODO
  - [x] Função de ler os dados (load_data)  
  - [ ] Testar load_data
  - [ ] Joins Functions


