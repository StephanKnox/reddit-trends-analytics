import minio
from minio import Minio
from minio.commonconfig import  CopySource
import datetime


class S3Helper():
    access_key: str
    secret_key: str
    endpoint: str
    path_to_raw: str
    path_to_stg: str


    def get_s3_client(self):
        return Minio(
                endpoint=self.endpoint, 
                access_key=self.access_key, 
                secret_key=self.secret_key, 
                secure=False)
    

    def list_files_in_s3(self, bucket_name, prefix=""):
        minio = self.get_s3_client()
        return minio.list_objects(bucket_name, prefix=prefix,recursive=True)    
    

    def upload_object_to_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        s3_client.fput_object(bucket_name, key, filename)


    def download_object_from_s3(self, bucket_name, key, filename):
        s3_client = self._get_s3_client()
        object = s3_client.fget_object(bucket_name, key, filename)
        return object
    

    def delete_objects_from_s3(self, bucket_name):
        objects_list = self.list_files_in_s3(bucket_name)
        minio = self._get_s3_client()

        for _obj in objects_list:       
            minio.remove_object(bucket_name, _obj._object_name)


    def move_object_between_buckets(self, src_bucket, tgt_bucket):
        s3_client = self._get_s3_client()
        today = datetime.now().date().strftime("%y%m%d")

        [ s3_client.copy_object(tgt_bucket, _obj.object_name, CopySource(src_bucket, _obj.object_name))
        for _obj in self.list_files_in_s3(src_bucket)
        if today in _obj.object_name ]