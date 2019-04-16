# Milestone

> XSS: 5 seconds setup, 20 seconds timeout  
> XS: 10 seconds setup, 120 seconds timeout  
> S: 30 seconds setup, 240 seconds timeout  
> M: 60 seconds setup, 600 seconds timeout  
> L: 120 seconds setup, 600 seconds timeout

## Data loading

Next: while reading file, store them to in memory arrays, as well as outputing to file.

How to handle input files:  
Extract file name, for each file, output meta data and binary file.

Also, output to file in page block?

## Simple table scan with a predicate

> Figure out how to do a table scan with a predicate. This means you need to be able to read that format you wrote out for step 1, check to see if each row matches a particular predicate, and then emit the matching rows. Make sure that however you "emit" rows is somewhat composable, for example, a Java Iterable or Iterator.

Except for large dataset, all other dataset fits inside memory, so... just read them in.

## Combine predicates

> Test your table scan code with a made up predicate on the XXXS data. Can you select all the rows from the A relation where the first column equals 4422? There should be only one match. How about rows where the second column is greater than 5000? What about an AND of those two predicates (zero results).

## Simple join

> Write a simple join algorithm, nested loop join or block nested loop join. This should make use of the table scan you wrote previously. Again, do this in a way that is composable, as you want your join operator to be able to read from a table scan or another join. Try to join the first column of A with the first column of B. Load these two relations in SQLite and see if you got the same results. Add predicates on other columns of A and B.

## Sum aggregator

> Write a sum aggregator. This will handle the SUM(A.cX), SUM(B.cY) part of the query. All it has to do is read rows and increment some accumulator variables. Once all of its input is gone, it can produce a single row with the sum results.

## Query optimizer

> Next, we need a query optimizer, but we have to build a few extra parts first. First, modify your data loader to collect metadata your optimizer will need (min/max, number of unique values, etc.). Make sure these modifications don't break the rest of your code. Second, write a query parser. This should take the input SQL queries and transform them into some representation that you can use inside of your optimizer. For example, you should probably try to associate non-join predicates with their base relations, and ensure that you have a list of all joins you need to perform.

> Now it is time to write the query optimizer. Using the metadata from your dataloader and the parsed representation of SQL queries, use some algorithm (Selinger's, a heuristic, something you make up yourself) to come up with a join ordering. I would suggest only considering left-deep orderings initially.

## Go further

> Once your optimizer can create join orderings, you should have all the pieces you need to get the right answer on the XXXS and possibly the XXS data. Glue your components together until you produce the right answer for these datasets. Much debugging will be required here. Try the XS, S datasets are well, see how far you can get with just your basic setup.
>
> You have a basic skeleton of the PA finished! Now it is time to optimize. The trick to optimization is profiling. Figure out what parts of your code are slow, i.e. where is your program spending all of its time? Is it in the join algorithm? Maybe switch to hash or sort block nested loop join. Is it in data loading? Make sure you aren't doing something silly. Is it in disk I/O time? Maybe think of a more compact way to store the data. Are your joins producing way too many rows? Maybe your query optimization algorithm has a bug.
>
> You should be able to push your basic data layout and a variant of sort block nested loop join at least as far as the C- benchmark. 
> 
> Now, it's time to go further! This is where you'll have to get creative. Can I change my data layout? Build indexes? Sort things ahead of time? Partition things by a hash key ahead of time? Somehow compress the data? Use parallelism in my join algorithms? Sky's the limit (although you can stop once you get an A!).
>
> A few notes on memory management:
> I know a lot of you have never really thought about memory before. If you are confused about "on disk" and "in memory," and how those concepts are expressed in code, you should schedule an appointment with a TA (or myself).
