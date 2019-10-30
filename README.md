# ETL from NHL API to SQLite Database

This repo contains a Python script ([*refresh_db.py*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/refresh_db.py)) that reads and operates on a dictionary of table specifications ([*table_specs.json*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/table_specs.json)) that define which endpoints from the NHL's publicly-available API ([documentation by Drew Hynes](https://gitlab.com/dword4/nhlapi)) can be accessed to organize a set of hockey data into a normalized relational database structure. The script pulls and transforms datasets to create the schema architecture and relies primarily on the *pandas* library to assemble and load representative DataFrames into a SQLite database file available for further querying and analysis ([*nhl.db*](https://github.com/joeylafyatis/nhl-api-etl/blob/master/nhl.db)).

## Data Flow Diagram

Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. 

![Data Flow Diagram](readme_diagrams/data_flow.svg)

## Entity-Relationship Diagram

There are many variations of passages of Lorem Ipsum available, but the majority have suffered alteration in some form, by injected humour, or randomised words which don't look even slightly believable. If you are going to use a passage of Lorem Ipsum, you need to be sure there isn't anything embarrassing hidden in the middle of text. All the Lorem Ipsum generators on the Internet tend to repeat predefined chunks as necessary, making this the first true generator on the Internet. 

### Core Metadata

It uses a dictionary of over 200 Latin words, combined with a handful of model sentence structures, to generate Lorem Ipsum which looks reasonable. The generated Lorem Ipsum is therefore always free from repetition, injected humour, or non-characteristic words etc.

![Entity-Relationship Diagram](readme_diagrams/entity_relationship.svg)

## Happy Birthday!

To the Vegas Golden Knights fan in my life that inspired this project :)
