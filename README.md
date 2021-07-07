# BigQuery Description Backupper

## What is this?

- Backup descriptions of Google BigQuery dataset, table and field to Google FireStore.
- Restore descriptions from FireStore to BigQuery

This tool can run on Python3.6+

## How to Use

#### 1.Install

```
git clone https://github.com/JapanTaxi/bqdesc_backupper.git
cd bqdesc_backupper
pip install -r requirements.txt
```

#### 2.Make Config File

copy sample

```
cp src/conf/config_sample.py src/conf/config.py
```

edit config

```
vim src/conf/config.py
```

#### 3.Run

backup all datasets and tables(fields) description in project to FireStore

```
python src/cli.py backup all
```

restore all

```
python src/cli.py restore all
```


## Functions

### Specify Table

backup table and fields description

```
python src/cli.py backup table -d myds -t mytable
```

restore 

```
python src/cli.py restore table -d myds -t mytable
```

### Specify Dataset

backup dataset description 

```
python src/cli.py backup dataset -d myds
```

restore 

```
python src/cli.py restore dataset -d myds
```

### FireStore Snapshot

For accidentally deletion, this tool provide function of FireStore snapshot.
By taking snapshot regularly, you can recover data from snapshot you want.


```
              production DB         snapshot DBs
                                    
BigQuery  -> FireStore(prod-db)  -> FireStore(prod-db-20191219)
                                    FireStore(prod-db-20191218)
                                    FireStore(prod-db-20191217)
```

#### take snapshot

```
python src/cli.py snapshot make
```

```
2019-12-19 19:52:39,173 [    INFO] Make FileStore snapshot collection
2019-12-19 19:52:39,173 [    INFO] copy bqdesc-backupper-table-desc -> bqdesc-backupper-table-desc-20191219
2019-12-19 19:52:44,053 [    INFO] copy bqdesc-backupper-dataset-desc -> bqdesc-backupper-dataset-desc-20191219
```


#### recover data from snapshot

recover table data

```
python src/cli.py snapshot recover-table -d myds -t mytable -s 20191219
```

recover dataset data

```
python src/cli.py snapshot recover-dataset -d myds -s 20191219
```


#### list snapshot

```
python src/cli.py snapshot list
```

## Other Functions

* [Integration with Slack](doc/SLACK.md)
* [Integration with Google Cloud Functions](doc/CLOUD_FUNCTIONS.md)

## For Developers

see [how to test document](test/HOW_TO_TEST.md)
