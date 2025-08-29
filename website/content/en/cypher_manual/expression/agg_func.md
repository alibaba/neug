# Aggregate Function

Aggregate Functions are primarily used to group current data and perform aggregation operations on elements within each group, ultimately producing only a single value for each group. The Aggregate Functions supported by NeuG are as follows:

Function | Description | Can be used with DISTINCT | Example
---------|-------------|---------------------------|--------
count | return the row counts | YES | Return count (a.name);
collect | collect the elements in a single list | YES | Return collect(a.name);
min | return the minimum value | NO | Return min(a.age);
max | return the maximum value | NO | Return max(a.age);
sum | sum up the value | NO | Return sum(a.age);
avg | return the average value | NO | Return avg(a.age);
