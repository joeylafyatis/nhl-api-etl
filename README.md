# ETL from NHL API to SQLite Database

[*refresh_db.py*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/refresh_db.py) iterates through a dictionary of table specifications ([*table_specs.json*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/table_specs.json)) to:

1. extract data from endpoints in the NHL's publicly-available API ([documentation by Drew Hynes](https://gitlab.com/dword4/nhlapi))
2. transform that data into *pandas* DataFrames
3. load those DataFrames as tables into a SQLite datbase file ([*nhl.db*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/nhl.db))

## Data Flow Diagram
![Data Flow Diagram](readme_diagrams/data_flow.svg)

## _table_specs.json_ Table Specifications

Table specifications implement the following structure:

```python
"table_name": {               #names SQL table and identifies non-standard refresh functions
  "standard_refresh": bool,   #whether the specification follows an identified standard pattern
  "api_endpoint": str,        #relevant NHL API endpoint from which to request data
  "select_col": [             #list of columns from DataFrame to be loaded into SQL table
    "columnA",
    "columnB",
    "columnC",
    ...
  ],     
  "rename_col": [             #list of headers for the columns in "select_col"
    "column_a",
    "column_b",
    "column_c",
    ...
  ],   
  "cast_dtypes": {            #dictionary of data types to cast for the columns in "rename_col"
    "column_b": "integer",
    "column_c": "integer"
  }
}
```

## _nhl.db_ Entity-Relationship Diagram
![Entity-Relationship Diagram](readme_diagrams/entity_relationship.svg)
