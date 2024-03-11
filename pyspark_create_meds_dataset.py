
def create_meds_dataset(
    spark_session: SparkSession,
    event_table: str,
    num_patients: int = None,
    time_cutoff: datetime = None,
) -> DataFrame:
    """
    create a pyspark dataframe in the meds schema
    using a SQL table of patient events
    
    args:
    - spark_session: pyspark session (SparkSession)
    - event_table: full name of the event table, including catalog and schema (e.g., "sandbox.femr_dev.events_10k")
    - num_patients: number of patients to include in the dataset. if None, include all patients
    - time_cutoff: only events prior to this datetime will be included

    returns:
    - a pyspark DataFrame object in meds format

    NOTE: This does not use the meds package at all. If the meds schema changes, we will need to update this code.
    TODO: we need to make sure that events are sorted in time.

    The final dataset should have the following schema, which should exaxely match the meds schema, but in pyspark format.

    root
    |-- patient_id: integer (nullable = true)
    |-- events: array (nullable = false) <-------------------- sorted by element.time
    |    |-- element: struct (containsNull = false)
    |    |    |-- time: string (nullable = true)
    |    |    |-- measurements: array (nullable = false)
    |    |    |    |-- element: struct (containsNull = false)
    |    |    |    |    |-- code: string (nullable = true)
    |    |    |    |    |-- text_value: string (nullable = true)
    |    |    |    |    |-- numeric_value: string (nullable = true)
    |    |    |    |    |-- datetime_value: string (nullable = true)
    |    |    |    |    |-- metadata: struct (nullable = false)
    |    |    |    |    |    |-- PAT_ID: string (nullable = true)
    |    |    |    |    |    |-- PAT_ENC_CSN_ID: long (nullable = true)
    |    |    |    |    |    |-- event_type: string (nullable = true)
    """

    # columns to include in the metadata struct. we could also make this an arg rather than hard-coded
    # NOTE: TODO: these are columns used by SHC. the list of metadata columns should probably be passed as an argument to this function
    metadata_columns = ["PAT_ID", "PAT_ENC_CSN_ID", "event_type"]

    # if a time cutoff is provided, filter using a where statement. TIMESTAMP
    time_cutoff_filter = ""
    if time_cutoff is not None:
        time_cutoff_filter = f" WHERE time < TIMESTAMP('{time_cutoff.isoformat()}')"

    # get the events table
    events_table = spark_session.sql(
        f"SELECT * FROM {event_table}" + time_cutoff_filter
    )

    if num_patients is not None:
        assert num_patients > 0, "num_patients must be positive or None"

        # select a subset of patients
        selected_patients = spark_session.sql(
            f"SELECT DISTINCT patient_id FROM {event_table} LIMIT {num_patients}"
        )
        events_table = events_table.join(
            selected_patients, on="patient_id", how="inner"
        )

    # take all required columns from the events table, and create a struct for metadata.
    # NOTE: currently we only populate "text_value", not "numeric_value" or "datetime_value"
    transformed_patient_rows = events_table.select(
        "patient_id",
        f.to_timestamp(events_table.time, "yyyy-MM-dd hh:mm:ss").alias("time"),
        "code",
        lit(None).cast(StringType()).alias("text_value"),
        events_table.value_num.alias("numeric_value"),
        lit(None).cast(StringType()).alias("datetime_value"),
        lit(f.struct(metadata_columns)).alias("metadata"),
    )

    # turn each row into a "measurement" struct, including code, all _value fields, and metadata
    measurement_list = transformed_patient_rows.select(
        "patient_id",
        "time",
        f.struct(
            "code", "text_value", "numeric_value", "datetime_value", "metadata"
        ).alias("measurement"),
    )

    # groupby over (patient_id, time) to get a list of all measurements for each patient at each timestamp
    # we want all events to be ordered by time for each patient, so here we order by time. This should ensure that
    # when we create agg_2, the list produced by collect_list is also ordered by time.
    agg_1 = (
        measurement_list.groupby("patient_id", "time")
        .agg(f.collect_list("measurement").alias("measurements"))
        .orderBy("time")
    )

    # create structs that contains (time, measurements) for each patient, these are called "events".
    # then, group by on patient_id to get a list of events.
    # NOTE: the list of events must be sorted by event.time. here we use f.sort_array to sort elements of each event list
    # when sorting structs, sort_array sorts by the first element of the struct, which in our case is time.
    agg_2 = (
        agg_1.select(
            "patient_id",
            f.struct("time", "measurements").alias("event"),
        )
        .groupby("patient_id")
        .agg(f.sort_array(f.collect_list("event")).alias("events"))
        .orderBy("patient_id")
    )

    return agg_2
