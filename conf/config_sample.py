class Config(object):

    #----------------------
    # Global
    #----------------------

    # Log Level
    #   valid choice : "info", "warn", "error", "debug"
    loglevel = "info"

    #----------------------
    # BigQuery
    #----------------------

    # GCP Project ID
    gcp_project = "your-gcp-project-id"

    # Use GCP service account key for GCP authentication or not.
    #   If True, use key json
    #   If False, use application defualt credential (https://cloud.google.com/docs/authentication/production#finding_credentials_automatically)
    gcp_use_key_json = False

    # GCP service account key json
    #   If gcp_use_key_json is True, this json file is used for authentication
    gcp_key_json = "/path/to/json"

    #-----------------------
    # FireStore
    #-----------------------

    # Collection to store dataset descriptions
    firesotre_dataset_desc_collection_name = 'bqdesc-backupper-dataset-desc'

    # Collection to store table descriptions
    firestore_table_desc_collection_name = 'bqdesc-backupper-table-desc'
    dataset_include_pattern = r'.*'

    #------------------------
    # Target Filter
    #------------------------

    # If dataset_id match this regexp, it is processed.
    #   Default r'.*'    (=match all)
    #
    #dataset_include_pattern = r'^(dwh_|raw_)'
    dataset_include_pattern = r'.*'

    # If dataset_id match this regexp, is is skiped.
    #   Default r'^$'    (= match nothing)
    #
    dataset_exclude_pattern = r'^$'

    # If table_id match this regexp, it is processed.
    #   Default r'.*'    (= match all)
    #
    table_include_pattern = r'.*'

    # If table_id match this regexp, is is skiped.
    #   Default r'^$'    (= match nothing)
    #
    table_exclude_pattern = r'^$'

