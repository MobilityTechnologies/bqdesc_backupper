## How to Test

cd root directory

```
cd bqdesc_backupper
```

make config

```
cp conf/config_sample.py conf/config_for_test.py
vim conf/config_for_test.py
```

make tables

```
test/make_test_dataset_and_tables.sh
```

run test

```
python -m unittest discover test "test_*.py"
```