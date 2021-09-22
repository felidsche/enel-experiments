import sys
import logging

from hdfs import InsecureClient
logger = logging.getLogger(__name__)  # holds the name of the module
logging.basicConfig(level=logging.INFO, filename=f"log/HDFSService.log",
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class HdfsService():
    def __init__(self, client):
        self.client = client

    def upload_file_to_hdfs(self, local_path: str, hdfs_path: str, overwrite: bool = False):
        logging.info(f"Uploading from: {local_path} to: {hdfs_path}, overwrite? {overwrite}")
        with open(local_path) as reader, self.client.write(hdfs_path=hdfs_path, overwrite=overwrite) as writer:
            for line in reader:
                if line.startswith('-'):
                    writer.write(line)
        logging.info(f"Finished Uploading from: {local_path} to: {hdfs_path} overwrite: {overwrite}")


if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("5 args are required")
        print("Usage: <operation: download/upload> <hdfs url> <hdfs user> <hdfs root dir> <hdfs dir>"
              "<local file path> <overwrite: True/False> (all string)")
    operation = sys.argv[1]
    url = sys.argv[2]
    user = sys.argv[3]
    root = sys.argv[4]
    hdfs_dir = sys.argv[5]
    client = InsecureClient(url=url, user=user, root=root)
    hdfs_service = HdfsService(client=client)
    if operation == "upload":
        file_path = sys.argv[6]
        overwrite = bool(sys.argv[7])
        hdfs_service.upload_file_to_hdfs(local_path=file_path, hdfs_path=hdfs_dir, overwrite=overwrite)


