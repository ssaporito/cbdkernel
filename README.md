# cbdkernel

Compilar:

make

Executar com benchmark:

./db --search-benchmark --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.csv

Busca por campo:

./db --search-field --field_name=name --field_value=Zazio  --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin

Mostrar registro numa dada posição:
./db --load-data --pos=333 --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin 

Imprimir todos os registros:
./db --print-bin --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin 







1. TODO
  - [x] Função de ler os dados (load_data)  
  - [ ] Testar load_data
  - [ ] Joins Functions


