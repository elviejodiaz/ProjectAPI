
# PROJECT API Architecture

![REST API Architecture](REST_API_Architecture.png)

## Overview

This document describes the architecture of a REST API built with FastAPI, which interacts with Azure SQL Database and Azure Storage Account. The REST API exposes three methods: `loadfiles`, `backupdata`, and `restore`.

## Components

### Client
The client initiates requests to the REST API. It can be any application or user that needs to interact with the data stored in Azure SQL Database and Azure Storage Account.

### REST API
The REST API is built using FastAPI and serves as the central component that processes client requests and interacts with Azure services.

### Azure Storage Account
Azure Storage Account is used to store CSV files and Avro files. It acts as the storage backend for the REST API.

### Azure SQL Database
Azure SQL Database is used to store the data in structured tables. It acts as the database backend for the REST API.

## Methods

### `loadfiles`
- **Client** sends a request to the REST API.
- **REST API** retrieves CSV files from Azure Storage.
- **REST API** loads the data into Azure SQL tables (`Jobs`, `Departments`, `HiredEmployees`).

### `backupdata`
- **Client** sends a request to the REST API.
- **REST API** fetches data from Azure SQL tables.
- **REST API** stores the data as Avro files in Azure Storage.

### `restore`
- **Client** sends a request to the REST API with the name of the target table (`Jobs`, `Departments`, `HiredEmployees`).
- **REST API** retrieves the corresponding Avro file from Azure Storage.
- **REST API** restores the data into the specified Azure SQL table.

## Diagram

The diagram above illustrates the interactions between the client, REST API, Azure Storage Account, and Azure SQL Database for each method (`loadfiles`, `backupdata`, `restore`).

# Company Database Architecture

![Company Database Star Schema](Company_Database_Schema.png)

## Overview

This document describes the architecture of the Company database hosted on Azure SQL. The database consists of several tables with defined relationships.

## Tables

### dbo.Departments
- **id**: INT, Primary Key
- **department**: NVARCHAR(4000)

### dbo.Jobs
- **id**: INT, Primary Key
- **job**: NVARCHAR(4000)

### dbo.HiredEmployees
- **id**: INT, NOT NULL
- **name**: NVARCHAR(4000)
- **datetime**: NVARCHAR(4000)
- **department_id**: INT, Foreign Key referencing dbo.Departments(id)
- **job_id**: INT, Foreign Key referencing dbo.Jobs(id)

### dbo.DimDate
- **DateKey**: INT, NOT NULL, PRIMARY KEY
- **FullDate**: DATE, NOT NULL
- **Day**: TINYINT, NOT NULL
- **DaySuffix**: CHAR(4), NOT NULL
- **Weekday**: TINYINT, NOT NULL
- **WeekdayName**: VARCHAR(10), NOT NULL
- **IsWeekend**: BIT, NOT NULL
- **DOWInMonth**: TINYINT, NOT NULL
- **DayOfYear**: SMALLINT, NOT NULL
- **WeekOfMonth**: TINYINT, NOT NULL
- **WeekOfYear**: TINYINT, NOT NULL
- **Month**: TINYINT, NOT NULL
- **MonthName**: VARCHAR(10), NOT NULL
- **Quarter**: TINYINT, NOT NULL
- **QuarterName**: VARCHAR(10), NOT NULL
- **Year**: INT, NOT NULL
- **MMYYYY**: CHAR(6), NOT NULL
- **FirstDayOfMonth**: DATE, NOT NULL
- **LastDayOfMonth**: DATE, NOT NULL
- **IsLeapYear**: BIT, NOT NULL

## Relationships

### dbo.HiredEmployees
- **department_id** references **dbo.Departments(id)**
- **job_id** references **dbo.Jobs(id)**

## Diagram

The diagram above illustrates the star schema architecture of the Company database, showing the relationships between the fact table (HiredEmployees) and dimension tables (Departments, Jobs, DimDate).
