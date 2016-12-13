'''
This Python Program connect to S3 and Openstack with single cpmmand through CLI
Program exposes generic API that abstract the interaction with S3 and Openstack Swift
Project Delivarables:
(I)nitialing Connection - Intialises connection between S3 and openstack swift
(C)reate bucket - Create a Bucket in  both S3 and Swift
(G)et object  - Fetches the object from swift and if not found pulls the object from s3
(a)rchieval set - Sets the archieval for buckets to s3 on timely basis
'''
import swiftclient
import boto3
import os
import shutil
import datetime
import time


# AWS glacier for archivel of an object from Openstack Swift
class valultclass():

    def __init__(self):
        try:
            self.glacier_conn= boto3.client('glacier')

        except Exception as e:
            print e
        else:
            print "Connection Successful to Public Cloud Cold Storage Infrastrcuture"

    def __str__(self):
        print self.glacier_conn
        return str(self.glacier_conn)

    def put_vault(self,vault_name):
        try:
            vault = self.glacier_conn.create_vault(vaultName=vault_name)
        except Exception as e:
            print e
            return 0
        else:
            print "Creating Vault",vault_name,vault
            return vault

    def upload_vault(self,vault_name,archieve_description,object_name):
        try:
            file_post=open(object_name, 'rb')
            response=self.glacier_conn.upload_archive(vaultName=vault_name,archiveDescription=archieve_description,body=file_post)

        except Exception as e:
            print e
        else:
            print response


class swiftclass():
# swift class for intearating get objects and put objects with the objects of openstack swift
    def __init__(self):
        try:
            self.swift_conn = swiftclient.client.Connection(authurl='http://172.31.32.72:5000/v2.0', user='admin', key='admin', tenant_name='demo', auth_version='2.0', os_options={'tenant_id': '551160ab61da4c4197349d91d9942ff6', 'region_name': ''})
            self.swift_conn.get_account()
        except Exception as e:
            print e
        else:
            print "Connection Successful to Onprem Object Storage Infrastrcuture"

    def __str__(self):
        print self.swift_conn
        return str(self.swift_conn)

    def put_container(self,containername):
        try:
            self.swift_conn.put_container(containername)
        except Exception as e:
            print e
        else:
            print "Container Added to OnPrem\nContainers:"
            bucket_list=self.swift_conn.get_account()[1]
            for i in bucket_list:
                print "On Prem Cloud: \t %s "%(dict(i)['name'])

    def get_object(self,container,object_name):
        openstack_files=[]
        try:
            resp_headers, obj_contents = self.swift_conn.get_object(container,object_name)
            with open(object_name, 'w') as local:
                local.write(obj_contents)
        except Exception as e:
            print e
            print "File Exception at On Prem Cloud,Fetching from Public Cloud"
            return 1
        else:
            return 0

    def put_object(self,container,object_name):
        try:
            file_post=open(object_name, 'rb')
            response=self.swift_conn.put_object(container, object_name, file_post)
        except Exception as e:
            print e
        else:
            print response

    def list_objects(self,container):
        try:
            print len(self.swift_conn.get_container(container)[1])," objects found  in on prem container : ",container
            for data in self.swift_conn.get_container(container)[1]:
                print '{0}\t{1}\t{2}'.format(data['name'], data['bytes'], data['last_modified'])
        except Exception as e:# coding=utf-8
            print e
# Intializing S3 though CLI

class s3class():

    def __init__(self):
        try:
            self.s3_conn = boto3.client('s3')
            list_buckets=self.s3_conn.list_buckets()
        except Exception as e:
            print e
        else:
            print "Connection Successful to Public Cloud Object Storage Infrastrcuture"
# Creating an object into container
    def put_container(self,containername):
        try:
            self.s3_conn.create_bucket(Bucket=containername)
        except Exception as e:
            print e
        else:
            print "Container Added to Public Cloud\nContainers:"
            bucket_list=dict(self.s3_conn.list_buckets())['Buckets']
            for i in bucket_list:
                print "On Public Cloud: \t %s "%(dict(i)['Name'])

    def __str__(self):
        print self.s3_conn
        return str(self.s3_conn)

# Retrieving an object from container
    def get_object(self,container,object_name):
        #get_object(self,container,object_name)
        print 'handling exception'
        response=self.s3_conn.get_object(Bucket=container,Key=object_name)
        print dict(response)
        with open(object_name, 'w') as f:
            chunk = response['Body'].read(1024*8)
            while chunk:
                f.write(chunk)
                chunk = response['Body'].read(1024*8)

# Listing an objects from bucket
    def list_object(self):
        response = self.s3_conn.list_objects(  Bucket='cmpe297')
        s3_files=[]
        for i in response['Contents']:
            s3_files.append(str(dict(i)['Key']))
        return s3_files

    def put_object(self,container,object_name):
        try:
            print 'posting data to s3'
            response=self.s3_conn.put_object(Body=open(object_name, 'rb'),Bucket=container,Key=object_name)
            print response

        except Exception as e:
            print e
        else:
            print response

 # Hybrid Manager interacts with s3 and Swift for inserting ,retrieving objects,managing archieval  .
class hybrid_manager(swiftclass,s3class,valultclass):

    def __init__(self):
        swiftclass.__init__(self)
        s3class.__init__(self)
        swiftclass.__str__(self)
        s3class.__str__(self)
        valultclass.__init__(self)
        valultclass.__str__(self)

        self.object_s3=[]
        self.object_swift=[]

    def put_container(self,container):
        s3class.put_container(self,container)
        swiftclass.put_container(self,container)

    def get_object(self,container,object_name=None):

# Geting an Object from Openstack and If error or exception fetch the same object from s3
        output = swiftclass.get_object(self,container,object_name)
        print output
        if (output==1):
            s3class.get_object(self,container,object_name)

    def archieve_objects(self,container):
        swiftclass.list_objects(self,container)
        vault_name=container+str(time.time())

        print "Archieving and Backing up to Public Cloud"
# Creating temporary directory for archieval
        try:
            if os.path.exists(container):
                shutil.rmtree(container, ignore_errors=True)
            os.makedirs(container)
        except OSError as e:
            print e
            pass
        for data in self.swift_conn.get_container(container)[1]:
            resp_headers, obj_contents = self.swift_conn.get_object(container,data['name'])
            filepath = os.path.join(container, data['name'])
            with open(filepath, 'w') as local:
                local.write(obj_contents)

        shutil.make_archive(container, 'gztar', container)
        valultclass.put_vault(self,vault_name)

        description_string="Archieving Data from Openstack Container"+datetime.datetime.now().isoformat()
        archieve_name=container+".tar.gz"
        valultclass.upload_vault(self,vault_name,description_string,archieve_name)
        print "Archieved"+container+"to Glacier "+datetime.datetime.now().isoformat()
# putting objects for archieval into glacicer
    def put_object(self,container,object_name):
        object_s3 = s3class.put_object(self,container,object_name)
        object_swift=swiftclass.put_object(self,container,object_name)

# the main program starts from here
if __name__=="__main__":

    while True:
        print("\n########################################################\n\nHybrid Cloud Manager Menu\n\nCMPE 297-03\n\n########################################################\n\n\n\n(I)nitialize\n(C)reateBucket\n(G)etobjects\n(P)utobject\n(A)rchieval\n((Q)uit")
        choice = raw_input(">>> ").lower().rstrip()
        if choice=="i":
            hm=hybrid_manager()
        elif choice=="c":
            bucketname=raw_input('Please Enter Bucket Name:').lower().rstrip()
            hm.put_container(bucketname)
        elif choice=="g":
            bucketname=raw_input('Please Enter Bucket Name:').lower().rstrip()
            object_name=raw_input('Please Enter Object Name:').lower().rstrip()
            hm.get_object(bucketname,object_name)
        elif choice=="p":
            bucketname=raw_input('Please Enter Bucket Name:').lower().rstrip()
            object_name=raw_input('Please Enter Object Name:').lower().rstrip()
            hm.put_object(bucketname,object_name)
        elif choice=="a":
            bucketname=raw_input('Please Enter Bucket Name to Archieve:').lower().rstrip()
            hm.archieve_objects(bucketname)
        elif choice=="q":
            break
        else:
            print("Invalid choice, please choose again\n")

    print("Execute the Program as python swift_program %s" %(__doc__))
