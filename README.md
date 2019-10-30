# ETL from NHL API to SQLite Database

**Table of Contents**

* [Data Flow Diagram](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#data-flow-diagram) 
* [Table Specifications](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#table-specifications) 
* [Entity-Relationship Diagram](https://github.com/joeylafyatis/nhl-api-etl/blob/master/README.md#entity-relationship-diagram)

[*refresh_db.py*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/refresh_db.py) iterates through a dictionary of table specifications ([*table_specs.json*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/table_specs.json)) that define (1) endpoints on the NHL's public API with which to extract data ([documentation by Drew Hynes](https://gitlab.com/dword4/nhlapi)), (2) the appropriate user-defined functions in *refresh_db.py* to transform that data into a *pandas* DataFrame, and (3) column, header, and data type definitions to load the resulting DataFrame into a SQLite datbase file ([*nhl.db*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/nhl.db)) so that it can be queried from the command line or using an RDBMS (I've been using the free tier of [DbVisualizer](https://www.dbvis.com/)).

## Data Flow Diagram
![Data Flow Diagram](readme_diagrams/data_flow.svg)

## Table Specifications

Table specifications are structured as follows:

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
To the Golden Knights fan in my life that inspired this project :)
