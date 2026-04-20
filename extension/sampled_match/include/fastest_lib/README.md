## Fastest [VLDB 2024]
**Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach** (VLDB 2024)

### Dependencies
- Boost Library
- g++ Compiler with C++20 support

### Usage 
Build with the command
```sh
mkdir build
cd build
cmake .. && make  
```

|     Argument      |                          Description                           |
|:-----------------:|:--------------------------------------------------------------:|
|        -d         |                        Dataset to solve                        |
|   --STRUCTURE X   |                   No substructure condition                    |
|   --STRUCTURE 3   |        Substructure filter condition : Triangle Safety         |
|   --STRUCTURE 4   | Substructure filter condition : Triangle and Four-cycle Safety |

Execution example with `yeast` dataset:
```sh
./FaSTest -d yeast
```
Which will first read queries from `yeast_ans.txt`, and evaluate all queries whose true cardinalities are known. 

The command 
```sh
./FaSTest -d yeast --STRUCTURE 3
```
disables Four-cycle safety.

### Datasets
The datasets and query graphs from [RapidMatch](https://github.com/RapidsAtHKUST/RapidMatch/) is used for evaluation.

#### Input Format 
```
t [#Vertex] [#Edge]
v [ID] [Label] [Degree]
v [ID] [Label] [Degree]
...
e [src.id] [dst.id] [EdgeLabel]
```
Edge labels are optional (missing labels are considered as zero).

Example (0-0-1-0 labeled path)
```
t 4 3
v 0 0 1
v 1 0 2
v 2 1 2
v 3 0 1
e 0 1
e 1 2
e 2 3
```

#### True Cardinalities
To assess accuracy, true cardinalities should be provided. We provide the ground truth values for 1,707 out of 1,800 queries for yeast dataset in `dataset/yeast/yeast_ans.txt`, computed with [DAF](https://github.com/SNUCSE-CTA/DAF). 