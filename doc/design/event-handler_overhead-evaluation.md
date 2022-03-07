# Event-Handler Overhead Evaluation


In advanced-analytics-framework  the Event-Handler is a Python UDF which is 
responsible for processing  given results of SQL queries in  and issues new SQL 
Queries. While performing these operations, the Event-Handler should be able to 
keep track of states of the process and needs to store them in the BucketFS. 

There might be a bit of overhead while loading/unloading state objects to/from 
BucketFS. In order to measure this overhead, the following experiments were 
carried out.

## Experiments Setup
In 2 different setups (1-node / 5 nodes Exasol cluster), different sized objects 
(20KB, 1MB, 25MB) are used by calling different BucketFS methods from UDF. Each 
experiment is executed 50 times and their statistical results are indicated 
in the below tables:

## Experiment Results

### 25MB Object
#### 1-node setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib  |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|:-------------------:|:-----------------------:|:-----------------------:|
| min - max | 0.85 - 1.14 | 0.20 - 0.42 | 5.09 - 7.06 | 3.21 - 4.75 |     0.03 - 0.04     |       0.02 - 0.04       |       2.71 - 4.29       |
| avg / std | 1.02 / 0.07 | 0.24 / 0.04 | 6.20 / 0.60 | 3.92 / 0.21 |     0.04 / 0.00     |       0.03 / 0.00       |       3.33 / 0.41       |

#### 5-nodes setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib  |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|:-------------------:|:-----------------------:|:-----------------------:|
| min - max | 1.11 - 2.51 | 0.14 - 0.15 | 6.24 - 8.21 | 3.16 - 5.06 |     0.01 - 0.02     |       0.02 - 0.05       |       3.02 - 4.84       |
| avg / std | 1.41 / 0.45 | 0.14 / 0.00 | 6.62 / 0.60 | 3.39 / 0.54 |     0.02 / 0.00     |       0.03 / 0.01       |       3.31 / 0.51       |




### 1MB Object
#### 1-node setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib  |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|:-------------------:|:-----------------------:|:-----------------------:|
| min - max | 0.13 - 1.01 | 0.01 - 0.02 | 0.90 – 1.03 | 0.19 - 0.32 |     0.01 - 0.01     |       0.01 - 0.01       |       0.16 - 0.39       |
| avg / std | 0.99 / 0.12 | 0.02 / 0.00 | 1.00 / 0.01 | 0.25 / 0.04 |     0.01 / 0.00     |       0.01 / 0.00       |       0.21 / 0.04       |
#### 5-nodes setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|:-------------------:|:-----------------------:|:----------------------:|
| min - max | 1.02 – 2.00 | 0.01 - 0.02 | 0.74 - 1.98 | 0.16 - 0.18 |     0.01 - 0.01     |       0.01 - 0.01       |      0.16 - 0.19       |
| avg / std | 1.10 / 0.23 | 0.01 / 0.00 | 1.19 / 0.26 | 0.17 / 0.03 |     0.01 / 0.00     |       0.01 / 0.01       |      0.17 / 0.04       |


### 20KB Object
#### 1-node setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | write <br>string | read <br>string | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib  |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|------------------|-----------------|:-------------------:|:-----------------------:|:-----------------------:|
| min - max | 0.90 - 2.00 | 0.05 - 0.07 | 0.91 - 1.10 | 0.04 - 0.06 | 0.81 - 1.13      | 0.03 - 0.05     |     0.01 - 0.01     |       0.01 - 0.01       |       0.01 - 0.01       |
| avg / std | 1.02 / 0.15 | 0.06 / 0,00 | 1.00 / 0.04 | 0.04 / 0.00 | 1.00 / 0.05      | 0.04 / 0.00     |     0.01 / 0.00     |       0.01 / 0.00       |       0.01 / 0.00       |

#### 5-nodes setup 
|  metrics  |    upload   |   download  |  serialize  | deserialize | write <br>string | read <br>string | local fs<br>to file | local fs <br>to fileobj | local fs <br>to joblib  |
|:---------:|:-----------:|:-----------:|:-----------:|:-----------:|------------------|-----------------|:-------------------:|:-----------------------:|:-----------------------:|
| min - max | 0.93 – 1.13 | 0.02 – 0.03 | 0.95 – 1.11 | 0.05 – 0.08 | 0.96 – 1.21      | 0.02 – 0.04     |     0.01 - 0.01     |       0.01 - 0.01       |       0.01 - 0.01       |
| avg / std | 1.00 / 0.12 | 0.02 / 0.00 | 1.02 / 0.09 | 0.05 / 0.01 | 1.00 / 0.05      | 0.03 / 0.00     |     0.03 / 0.00     |       0.01 / 0.00       |       0.01 / 0.00       |
