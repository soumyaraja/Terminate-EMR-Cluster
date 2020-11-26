# Terminate-EMR-Cluster
## Pre-requisite
* create lambda function
* create EMR roles and required access to lambda to spin up EMR cluster
* create trigger for lambda function
* create required lambda envrionment varibale(s)

## Steps
* lambda function to shut down  EMR cluster
* provides necessary delay (30-60 sends delay to complete all necessay steps for unallocation in hadoop cluser pointer) though hadopp output already received in aws bucket
