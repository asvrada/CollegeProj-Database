# Design

## What should program do

There are two main parts in this program: Indexing and Querying.

### Indexing

Receive paths to csv files from stdin, each path seperated by `,`.

For each csv file:

* Open and read
	* Hand-write CSV parser
	* Merge sort if can't fit into memory?
* Indexing
* Write the result of indexing back to disk
	* B+ Tree?

### Querying

Receive SQL queries in string, seperated by newline, from stdin.

For each query string:

* Parse the query
	* SQL parser
* Execute the query
	* Deep Left Join, to find use DP
* Output result to stdout

## Components

1. data loader
2. parser
3. catalog
4. optimizer
5. execution engine

### Data loader
**Input**: 

path to each csv file

**Output**:

Some better format of these data

For each relation (csv file), we generate one file. The output file should contains two parts: metadata and data.

#### Metadata

Preferably size of multiple of 4KB (page size).

Stores metadata like catalog, B-tree (if possible), etc.

Data layout (all unit in bytes):

size of metadata (multiple of size of block, 1 means sizeof(metadata) == 4096 Bytes)

number of column

### Parser
**Input**: 

SQL query

**Output**:  

1. Column to select  
1. Relation to join (and on which column)  
2. Predicates

### Catalog

Store metadata about relation and column, like min, max, number of unique value.

### Optimizer

**Input**: SQL

**Output**: Join order, tree of operators (relational algebra)

Decide the join order

### Execution Engine

Actually do the job.

Block nested join, etc.