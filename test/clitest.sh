python main.py backup table -d test_bqdesc_buckuper -t both_table_and_filed_have_desc
python main.py restore table -d test_bqdesc_buckuper -t both_table_and_filed_have_desc

python main.py backup dataset -d test_bqdesc_buckuper
python main.py restore dataset -d test_bqdesc_buckuper

python main.py backup all
python main.py restore all

python main.py snapshot make
python main.py snapshot list

python main.py snapshot recover-table -d test_bqdesc_buckuper -t both_table_and_filed_have_desc -s 20191219
python main.py snapshot recover-dataset -d test_bqdesc_buckuper -s 20191219
