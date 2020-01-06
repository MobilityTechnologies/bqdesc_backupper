# Google Cloud Functions Integration

Run this tool on [Google Cloud Functions](https://cloud.google.com/functions/docs/quickstart-python?hl=ja)

### Deploy this tool to Cloud Functions

1.Install gcloud commnad

2.Run

```
gcloud beta functions deploy bqdesc_backupper --source=src --runtime "python37" --trigger-http --entry-point cloud_functions_main --timeout 540
```

### Invoke Cloud Function

```
curl -X POST -d '(COMMAND_JSON)' -H "Content-Type:application/json" https://xxxx-yyyy.cloudfunctions.net/bqdesc_backupper 
```

#### COMMAND_JSON

backup table description

```json
{"action":"backup_table", "dataset":"MY_DATASET", "table":"MY_TABLE"}
```

backup dataset description

```json
{"action":"backup_dataset", "dataset":"MY_DATASET"}
```

backup all description

```json
{"action":"backup_all"}
```

restore table description

```json
{"action":"restore_table", "dataset":"MY_DATASET", "table":"MY_TABLE"}
```

restore dataset description

```json
{"action":"restore_dataset", "dataset":"MY_DATASET"}
```

restore all description

```json
{"action":"restore_all"}
```

take DB snapshot

```json
{"action":"snapshot_make", "dataset":"MY_DATASET", "table":"MY_TABLE"}
```

recover 

```json
{"action":"snapshot_recover_table", "dataset":"MY_DATASET", "table":"MY_TABLE", "snapshot": "YYYYMMDD"}
```

```json
{"action":"snapshot_recover_dataset", "dataset":"MY_DATASET", "table":"MY_TABLE", "snapshot": "YYYYMMDD"}
```
