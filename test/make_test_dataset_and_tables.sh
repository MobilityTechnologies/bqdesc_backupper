export PATH=/Users/nzk190130g/.pyenv/versions/2.7.15/bin:$PATH

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  ${TEST_DATASET}

#----------------------------------------------------

cat <<EOS > ddl.json
[
  {
    "description": "this is col1 desc",
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  }
]
EOS


bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 --description "this is table desc" \
 ${TEST_DATASET}.both_table_and_filed_have_desc

#--------------------------------------------------


cat <<EOS > ddl.json
[
  {
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 --description "this is table desc" \
 ${TEST_DATASET}.table_only_have_desc

#--------------------------------------------------


cat <<EOS > ddl.json
[
  {
    "description": "this is col1 desc",
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 ${TEST_DATASET}.col_only_have_desc


#--------------------------------------------------

cat <<EOS > ddl.json
[
  {
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 ${TEST_DATASET}.neither_table_nor_filed_have_desc


#--------------------------------------------------

cat <<EOS > ddl.json
[
  {
    "description": "this is col1 desc",
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  },
  {
      "mode": "REQUIRED",
      "name": "col2",
      "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 ${TEST_DATASET}.part_of_cols_have_desc


#--------------------------------------------------

cat <<EOS > ddl.json
[
  {
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  },
  {
      "mode": "REQUIRED",
      "name": "col2",
      "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 ${TEST_DATASET}.update_test

#--------------------------------------------------

cat <<EOS > ddl.json
[
  {
    "description": "this is col1 desc",
    "mode": "REQUIRED",
    "name": "col1",
    "type": "STRING"
  },
  {
    "description": "this is col2 desc",
    "mode": "REQUIRED",
    "name": "col2",
    "type": "STRING"
  },
  {
    "description": "this is col3 desc",
    "mode": "REQUIRED",
    "name": "col3",
    "type": "STRING"
  }
]
EOS

bq mk --project_id ${GOOGLE_CLOUD_PROJECT}  --schema ddl.json \
 ${TEST_DATASET}.three_filed_descriptions


rm ddl.json