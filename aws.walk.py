import os

import boto
import boto3
import sys

SOURCE_DIR = '/home/sudip/myProjects.aws'
MAX_SIZE = 6 * 1000 * 1000
PART_SIZE = 3 * 1000 * 1000

def create_bucket(bucket_name):
    retVal=0
    s3=boto.connect_s3()

    bucket=s3.lookup(bucket_name)

    if bucket:
        print "Bucket (%s) already exists" % bucket_name
        bucket=s3.get_bucket(bucket_name)
        retVal=1
    else:
        try:
            bucket=s3.create_bucket(bucket_name)
        except s3.provider.storage_create_error, e:
            print "Bucket (%s) is owned by another user, exiting" % bucket_name
            retVal=-1
            sys.exit(1)
            
    return bucket, retVal


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


def traverseSource(sourceDir):
    print "entering here"
    files=[]
    for root, dirName, fileName in os.walk(sourceDir):
        for fileNm in fileName:
            files.append(os.path.join(root,fileNm))
    return files

bucket,_ = create_bucket("sudip_2017_08")
s3=boto.connect_s3()
s3c=boto3.client('s3')
uploadFileNames = []
for root, dirName, fileName in os.walk(SOURCE_DIR):
    for fileNm in fileName:
        fname=os.path.join(root, fileNm)
	uploadFileNames.append(fname)
    
    # start uploading
    for filename in uploadFileNames:
        sourcepath = filename
        destpath = filename

        filesize = os.path.getsize(sourcepath)
        if filesize > MAX_SIZE:
            print "multipart upload"
            mp = bucket.initiate_multipart_upload(destpath)
            fp = open(sourcepath,'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                print "uploading part %i" %fp_num
                mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

            mp.complete_upload()

        else:
            print "singlepart upload"
            k = boto.s3.key.Key(bucket)
            k.key = destpath
            k.set_contents_from_filename(sourcepath,
                    cb=percent_cb, num_cb=10)

