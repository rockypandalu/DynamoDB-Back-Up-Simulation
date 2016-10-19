# DynamoDB-Back-Up-Simulation
Simulate the behavior of DynaomDB and DynamoDB Streams during table back-up.

##Things to notice
1. Client can only modify the value in each item, they can not modify the key
2. Client communicates to DynamoDB through put and delete which are idempotent, and no log will be generated in DynamoDB Streams if the process did not change any value in table
