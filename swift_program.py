'''
Python Program to connect to S3 and Openstack .
Program exposes generic API that abstract the interaction with S3 and Openstack Swift
Initialize Connection to s3 and Swift

Todo List
Initialize Connection:
Create bucket -Create a Bucket in  both S3 and Swift
List Buckets - List the abstract bukets list.
Insert object [BucketName,object] - Inserts the Object in s3 and Swift
Get object [BucketNamE,object]  - Fetches the object from swift and if not found pulls the object from s3
Set archieval [BucketName,Time] - Sets the archieval for buckets to s3 on timely basis
Set retention [Bucket,Time]   - Deletes the old data from openstack as per retention time.

'''

import swiftclient
import boto3

class swiftclass():
    '''
    swift class for interating [get objects and put objects ]with the objects of openstack swift

    '''
    def __init__(self):
        try:
            self.swift_conn = swiftclient.client.Connection(authurl='http://172.31.56.197:5000/v2.0', user='admin', key='admin', tenant_name='demo', auth_version='2.0', os_options={'tenant_id': '4d8ef3068caa44cc9e030635c3755777', 'region_name': ''})
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
            print "File Exception at On Prem Cloud ,Fetching from Public Cloud"
            print "1"
            return 1
        else:
            return 0

        '''
        for data in self.swift_conn.get_object('container')[1]:
                openstack_files.append(str(data['name']))
        return openstack_files
        '''

    def put_object(self,container,object_name):
        try:
            file_post=open(object_name, 'rb')
            response=self.swift_conn.put_object(container, object_name, file_post)
        except Exception as e:
            print e
        else:
            print response

class s3class():
    '''
    s3 class for interating [get objects and put objects ]with the objects of openstack swift
    '''
    def __init__(self):
        try:
            self.s3_conn = boto3.client('s3')
            list_buckets=self.s3_conn.list_buckets()
        except Exception as e:
            print e
        else:
            print "Connection Successful to Public Cloud Object Storage Infrastrcuture"

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






    def list_object(self):
        response = self.s3_conn.list_objects(  Bucket='cmpe297')
        s3_files=[]
        for i in response['Contents']:
            s3_files.append(str(dict(i)['Key']))
        return s3_files

    def put_object(self,container,object_name):
        try:
            response=self.s3_conn.put_object(Body=open(object_name, 'rb'),Bucket=container,Key=object_name)
        except Exception as e:
            print e
        else:
            print response


class hybrid_manager(swiftclass,s3class):
    '''
    Hybrid Manager for interacting with s3 and Swift for inserting and retrieving objects,managing archieval
    scheduling backups .
    '''
    def __init__(self):
        swiftclass.__init__(self)
        s3class.__init__(self)
        swiftclass.__str__(self)
        s3class.__str__(self)

        self.object_s3=[]
        self.object_swift=[]

    def put_container(self,container):
        s3class.put_container(self,container)
        swiftclass.put_container(self,container)

    def get_object(self,container,object_name=None):
        '''
        Get the Object from Openstack and If error or exception fetch the same object from s3

        '''
        output = swiftclass.get_object(self,container,object_name)
        print output
        if (output==1):
            s3class.get_object(self,container,object_name)


        def put_object(self,container,object_name):
            object_s3 = s3class.put_object(self,container,object_name)
            object_swift=swiftclass.put_object(self,container,object_name)


if __name__=="__main__":

    while True:
        print("\nMenu\n\n(I)nitialize\n(C)reateBucket\n(G)etobjects\n(P)utobject\n(Q)uit")
        choice = raw_input(">>> ").lower().rstrip()
        if choice=="i":
            hm=hybrid_manager()
        elif choice=="c":
            bucketname=raw_input('Enter Bucket Name:').lower().rstrip()
            hm.put_container(bucketname)
        elif choice=="g":
            bucketname=raw_input('Enter Bucket Name:').lower().rstrip()
            object_name=raw_input('Enter Object Name:').lower().rstrip()
            hm.get_object(bucketname,object_name)
        elif choice=="p":
            bucketname=raw_input('Enter Bucket Name:').lower().rstrip()
            object_name=raw_input('Enter Object Name:').lower().rstrip()
            hm.put_object(bucketname,object_name)
        elif choice=="q":
            break
        else:
            print("Invalid choice, please choose again\n")

    print("Execute the Program as python swift_program %s" %(__doc__))
