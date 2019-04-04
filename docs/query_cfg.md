# SQL Query format in CFG

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
