# cbdkernel

Compilar:

make

Converter um csv em binário:
./db --schemadb=../data/schema/schemadb.cfg --schema=1 --convert --in ../data/csv/telephones.csv --out ../data/csv/telephones.bin

Busca com benchmark:

./db --search-benchmark --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.csv

Busca por campo:

./db --search-field --field_name=name --field_value=Zazio  --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin

Mostrar registro numa dada posição:

./db --load-data --pos=333 --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin 

Imprimir todos os registros:

./db --print-bin --schemadb=../data/schema/schemadb.cfg --schema 0 --in ../data/csv/company_small.bin 

Join:

./db --join --join-type=[natural_inner|natural_left|natural_right|natural_full] --join-impl=[nested|nested_existing_index|nested_new_index|merge|hash] --field_name=name --schemadb=../data/schema/schemadb.cfg --schema 0 --schema2 1 --in ../data/csv/company_small.bin --in2 ../data/csv/telephones.bin [--indexfile=../data/csv/schema1.index --indexfile2=../data/csv/schema2.index]

Join com benchmark:

./db --join-benchmark --field_name=name --schemadb=../data/schema/schemadb.cfg --schema 0 --schema2 1 --in ../data/csv/company_small.bin --in2 ../data/csv/telephones.bin


1. TODO
  - [x] Função de ler os dados (load_data)  
  - [x] Testar load_data
  - [x] Joins (natural inner, natural left, natural right, natural full)
  - - [x] Nested join
  - - [x] Nested join with existing index
  - - [x] Nested join with new index
  - - [x] Merge join
  - - [x] Hash join


