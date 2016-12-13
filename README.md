# Python-HybridStorageManager
Python Hybrid Storage Manager for Openstack and Swift


We intend to implement a command line object storage management solution for achieving the

above side features to configure policy of scalability, reliability [DR site], data retention policy

for object storage in private cloud. The project would be implemented using OpenStack swift

storage and AWS S3 and glacier storage. There is no option for cold storage available in

Openstack and our project provides a functionality to achieve the feature of cold storage using

AWS glacier along with providing the above said benefits. The following tool complements the

Openstack object storage features of cross site replication to achieve hybrid approach to

storage management by extending the on premise swift architecture to cloud.


This Python Program connect to S3 and Openstack with single cpmmand through CLI
Program exposes generic API that abstract the interaction with S3 and Openstack Swift
Project Delivarables:
(I)nitialing Connection - Intialises connection between S3 and openstack swift
(C)reate bucket - Create a Bucket in  both S3 and Swift
(G)et object  - Fetches the object from swift and if not found pulls the object from s3
(a)rchieval set - Sets the archieval for buckets to s3 on timely basis


GOALS

● Hybrid cloud storage to help deploy data storage both on the public cloud and on

premise.

● Scaling the onprem infrastructure to public cloud

● Policy to configure Flexible data retention policy

● Automating backup and archival to the public cloud

● The flexibility to choose which deployment works best for the data workloads

● To provide adaptive object storage that can be scaled and adjusted on a workload basis

● A simplified and streamlined object storage that uses similar unified technology both on

the premise and on the public cloud

● Ability to manage unpredictable data growth along with balancing storage costs,

performance and compliance demands
