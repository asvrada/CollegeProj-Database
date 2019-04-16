# Design

## What should program do

There are two main parts in this program: Indexing and Querying.

They are handled by `load_csv_file` and `execute` respectively.

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

### Data loader (todo)
**Input**: 

path to each csv file

**Output**:

Some better format of these data.

For each relation (csv file), we generate two kinds of files:
1. binary file, one for each column
2. Metadata file, one for each relation

#### Binary file

Naming: {relation}{column index}.binary  
For example: A0.binary, B11.binary

We generate one binary file for each column, they are page aligned as long as each int takes 4 bytes to store, we don't care if the last page is in full length (say, 8KB), since reading it shouldn't be a problem.

Possible maximum size: 100MB with 25,000,000 int/row. This means for each relation, we can always fit one of it's entire columns into memory. 

> With up to 26 relations, that would cost 2600MB. But not all relations are that large, so it's resonable.

#### Metadata

Preferably size of multiple of 4KB (page size).

Stores metadata like catalog, B-tree (if possible), etc.

Data layout (all unit in bytes):

size of metadata (multiple of size of block, 1 means sizeof(metadata) == 4096 Bytes)

number of column

### Parser (done)
**Input**: 

SQL query

**Output**:  

1. Column to select  
2. Relation to join (and on which column)  
3. Predicates

### Catalog

Store metadata about relation and column, like min, max, number of unique value.

### Optimizer (todo)

**Input**: SQL

**Output**: Join order, tree of operators (relational algebra)

Decide the join order

### Execution Engine

#### Select

#### Join 
Sorted Block Nested Loop Join

Todo: with B-tree, we could find if a number is in the relation or not much quicker, without going through the whole file like what we did currently.

#### Sum