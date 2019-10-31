# ETL from NHL API to SQLite Database

**Table of Contents**

* [Data Flow Diagram](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#data-flow-diagram) 
* [Table Specifications](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#table-specifications) 
* [Entity-Relationship Diagram](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#entity-relationship-diagram)

This repo contains a Python script ([*refresh_db.py*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/refresh_db.py)) that iterates through a dictionary of table specifications (in [*table_specs.json*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/table_specs.json)) to (1) extract data available from endpoints in the NHL's API ([documentation by Drew Hynes](https://gitlab.com/dword4/nhlapi)), (2) transform that data into *pandas* DataFrames, and (3) load those DataFrames as tables into a SQLite datbase file ([*nhl.db*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/nhl.db)) so that it can be queried from the command line or using an RDBMS (I've been using the free tier of [DbVisualizer](https://www.dbvis.com/)).

## Data Flow Diagram
![Data Flow Diagram](readme_diagrams/data_flow.svg)

## Table Specifications

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

## Entity-Relationship Diagram
![Entity-Relationship Diagram](readme_diagrams/entity_relationship.svg)

## Happy Birthday!
to the Golden Knights fan in my life that inspired this project :)
