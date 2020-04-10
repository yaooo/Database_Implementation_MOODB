# MooDB mini-project database 2019-2020

### Prerequisites
* Have Java installed.
* Make sure files are structured like this structure tree below:
```bash
.
├── dataset
│   ├── sf10.csv
│   ├── sf11.csv
│   ├── sf12.csv
│   ├── sf13.csv
│   ├── sf14.csv
│   ├── sf15.csv
│   ├── sf16.csv
│   ├── sf17.csv
│   ├── sf18.csv
│   ├── sf19.csv
│   ├── sf1.csv
│   ├── sf20.csv
│   ├── sf2.csv
│   ├── sf3.csv
│   ├── sf4.csv
│   ├── sf5.csv
│   ├── sf6.csv
│   ├── sf7.csv
│   ├── sf8.csv
│   └── sf9.csv
├── Main.java
├── NaiveQueryBatch.java
├── NaiveSchema.java
├── NaiveStorage.java
├── QueryBatch1.java
├── QueryBatch2.java
├── QueryBatch3.java
├── Query.java
├── README.MD
├── Schema.java
└── Trie.java

```
### Compile the code
```
javac Main.java
javac *.java
```

## Run the tests

To check the query evaluation results for all .csv files, using NaiveDB, MooDB-v1, MooDB-v2, and MooDB-v3. 
Print out the return values for each query, ordered by file name.
```
java Main p
```


To check the query evaluation results for all .csv files, using NaiveDB, MooDB-v1, MooDB-v2, and MooDB-v3.
Not print out the return values but only the run time.
```
java Main
```


Benchmark Question 3.a
```
java Main a
```

Benchmark Question 3.b
```
java Main b
```

Benchmark Question 3.c
```
java Main c
```

Benchmark Question 3.d
```
java Main d
```

Benchmark Question 3.e
```
java Main e
```