## OddHunter
This is a toy project using Apache Spark, Amazon S3 and Amazon EMR.

### Problem Statement
Dataset contains integer key-value pairs in multiple files in CSV or TSV formats in an S3 bucket. 
For any given key across the entire dataset, there exists exactly one value that occurs an odd number of times.

Write an app using Spark, which for each key will find the value that occurs an odd number of times and write the result
to an S3 bucket.

Run you app both locally and on Amazon EMR.

### Solution description
Let's note that the problem comes down to grouping the input by key, and then for each group finding the value that 
exists an odd number of times. Since values are integers we can do that by reducing each group to a single integer 
(the result) with a bitwise XOR operation - even occurrences of integers will cancel out and only the number that appeared
odd number of times will be left.

Let's note that bitwise XOR operation on integers is commutative and associative, so it is good for parallelization.

#### Time complexity
Time complexity of this solution is O(N), where N is the number of key->value pairs in input. We need to go through the
entire dataset only once. This cannot be improved, because we need to examine each pair.

#### Space complexity
Space complexity of this solution is O(K), where K is the number of unique keys in input. Finding the result of each 
group is O(1), because we only need 1 accumulator integer for the reduce operation, but we need to solve this for each group,
of which there can be many. Worst case scenario is that all groups have only 1 element, which would mean that the solution 
takes O(N) space, but that is still linear space.

#### Data distribution and shuffling
We have no knowledge of the data other than its format, so there is no input for considering data-locality optimizations. 
To solve the problem shuffle transformation is required, because values for given key may be scattered across different 
partitions. We use the `reduceByKey`, which under the hood reduces groups locally first, and only after that 
it shuffles the data for the final reduction step - this reduces the amount of data that needs to be sent across the network. 
This optimization will be:
* more effective if there are fewer keys with multiple values
* more effective if same keys end up grouped on partitions (the fewer partitions per key the less data need to be shuffled)
* less effective when there are not many values per key
* less effective when keys are scattered randomly across all partitions
 
#### Build process, jar packaging
As this is a toy project the build and deployment processes are not optimized. Improvements that could be made include 
producing smaller jar files (eg. jar that is sent to EMR does not need to include all 3 Main objects) and automating the 
process. Creating ephemeral EMR cluster and tearing it down after the job is finished could also be considered.

Parsing of args and using config files rather than program arguments could be improved as well.

## Usage

###Local
```
sbt "runMain io.ugon.oddhunter.OddHunterLocalSolver --input <S3_LOCATION> --output <S3_LOCATION> --aws-profile <AWS_PROFILE>" 
```
Or use `--aws-use-default-provider-chain` instead of `--aws-profile <AWS_PROFILE>`


###EMR
1. Build jar for submitting to EMR
```
sbt package
```

2. Submit job to EMR
```
AWS_REGION=<AWS_REGION> sbt "runMain io.ugon.oddhunter.OddHunterEmrSubmitter --input <S3_LOCATION> --output <S3_LOCATION> --job-jar <JAR_LOCAL_LOCATION> --aws-profile <AWS_PROFILE> 
 --job-flow-id <JOB_FLOW_ID> --s3-jar-location <S3_JAR_LOCATION> --s3-bucket <S3_BUCKET> --s3-jar-file <S3_JAR_FILE>
```
Or use `--aws-use-default-provider-chain` instead of `--aws-profile <AWS_PROFILE>`


