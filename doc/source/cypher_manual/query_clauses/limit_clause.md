# Limit Clause

Limit is used to control the number of output results. In addition to being used alone, it can also be used together with Order BY for TopK operations. NeuG currently supports two types of expressions in Limit: 1. A single Integer constant. 2. Arithmetic expressions with only constant parameters.

## Limit with Integer Value

```
Match (a:person)
Return a.age
Limit 2;
```
Since there is no ordering of the output, the result may be any two results.

output:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

## Limit with Integer Expression

```
Match (a:person)
Return a.age
Limit 1+1;
```

output:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

# Skip Clause

Limit controls the number of output results, which is equivalent to determining the output result row number range as [0, upper_bound). Skip controls skipping the first few rows of output results, which is equivalent to determining the output result row number range as [lower_bound, +∞), where we assume row numbers start from 0.

## Skip with Integer Value

```
Match (a:person)
Return a.age
Skip 2;
```
The query is used to skip the first two rows of results.

output:
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Skip with Integer Expression

```
Match (a:person)
Return a.age
Skip 1+1;
```

output:
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```


