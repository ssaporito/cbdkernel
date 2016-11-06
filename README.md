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

Join:

--join --join-type=natural_inner --join-impl=nested --field_name=name --schemadb=../data/schema/schemadb.cfg --schema 0 --schema2 1 --in ../data/csv/company_small.bin --in2 ../data/csv/telephones.bin






1. TODO
  - [x] Função de ler os dados (load_data)  
  - [x] Testar load_data
  - [ ] Joins (natural inner, natural left, natural right, natural full)
  - - [ ] Nested join
  - - [ ] Nested join with existing index
  - - [ ] Nested join with new index
  - - [ ] Merge join
  - - [ ] Hash join


