{{
    codegen.generate_source(
        schema_name = 'ALUMNO15',
        database_name = 'ALUMNO15_DEV_BRONZE',
        generate_columns = True,
        include_descriptions=True,
        include_data_types=True,
        include_database=True,
        include_schema=True,
        case_sensitive_databases=False,
        case_sensitive_schemas=False,
        case_sensitive_tables=False,
        case_sensitive_cols=False,
        )
}}