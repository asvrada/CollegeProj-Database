# Design

## What the assignment requires from the program

There are two parts of the program: Indexing and Querying.

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

## Input format in CFG

Input -> `Paths``Newlines``Number``Newlines``Queries`

Paths -> `Path` | `Path`,`Whitespaces``Paths`  

// this is the data format for CSV files  
Data -> 

Whitespaces -> None | `Whitespace` | `Whitespace``Whitespaces` 
  
Queries -> `Query` | `Query``Newlines``Queries`  

Newlines -> `Newline` | `Newline``Newlines`  

Query -> `FirstLine``Newline``SecondLine``Newline``ThirdLine``Newline``FourthLine`

FirstLine -> SELECT`Whitespace``Sums`

Sums -> `Sum` | `Sum`,`Whitespace``Sums`

Sum -> SUM(`Relation`.`Column`)

SecondLine -> FROM`Whitespace``Relations`

Relations -> `Relation` | `Relation`,`Whitespace``Relations`

ThirdLine -> WHERE`Whitespace``Joins`

Joins -> `Join` | `Join``Whitespace`AND`Whitespace``Joins`

Join -> `Relation`.`Column``Whitespace`=`Whitespace``Relation`.`Column`

FourthLine -> AND`Whitespace``Predicates`; | AND`Whitespaces`;

Predicates -> `Predicate` | `Predicate``Whitespace`AND`Whitespace``Predicates`

Predicate -> `Relation`.`Column``Whitespace``Operator``Whitespace``Number`

Operator -> = | > | <
