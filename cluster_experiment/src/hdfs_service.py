import logging
import sys

from hdfs import InsecureClient

logger = logging.getLogger(__name__)  # holds the name of the module
logging.basicConfig(level=logging.INFO, filename=f"../../log/HDFSService.log",
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def main(op: str):
    url = sys.argv[2]
    user = sys.argv[3]
    root = sys.argv[4]

    client = InsecureClient(url=url, user=user, root=root)

    hdfs_service = HdfsService(client=client)

    if op == "file_upload":
        hdfs_path = sys.argv[5]
        file_path = sys.argv[6]
        try:
            overwrite = bool(sys.argv[7])
        except IndexError:
            print("overwrite defaults to False")
            overwrite = False
        hdfs_service.upload_file_to_hdfs(local_path=file_path, hdfs_path=hdfs_path, overwrite=overwrite)

    if op == "mkdir":
        hdfs_path = sys.argv[5]
        hdfs_service.create_dir_on_hdfs(hdfs_path=hdfs_path)

    if op == "rename":
        hdfs_src_path = sys.argv[5]
        hdfs_dst_path = sys.argv[6]
        hdfs_service.rename_file(hdfs_src_path, hdfs_dst_path)


class HdfsService():
    def __init__(self, client):
        self.client = client

    def upload_data_to_hdfs(self, local_path: str, hdfs_path: str, overwrite: bool = False):
        logging.info(f"Uploading data from: {local_path} to: {hdfs_path}, overwrite? {overwrite}")
        with open(local_path) as reader, self.client.write(hdfs_path=hdfs_path, overwrite=overwrite) as writer:
            for line in reader:
                if line.startswith('-'):
                    writer.write(line)
        logging.info(f"Finished Uploading data from: {local_path} to: {hdfs_path} overwrite: {overwrite}")

    def upload_file_to_hdfs(self, local_path: str, hdfs_path: str, overwrite: bool = False):
        logging.info(f"Uploading file from: {local_path} to: {hdfs_path}, overwrite? {overwrite}")
        self.client.upload(hdfs_path=hdfs_path, local_path=local_path, overwrite=overwrite)
        logging.info(f"Finished Uploading file from: {local_path} to: {hdfs_path} overwrite? {overwrite}")

    def create_dir_on_hdfs(self, hdfs_path: str):
        logging.info(f"Creating a directory at: {hdfs_path}")
        self.client.makedirs(hdfs_path=hdfs_path)
        logging.info(f"Finished creating a directory at: {hdfs_path}")

    def rename_file(self, hdfs_src_path: str, hdfs_dst_path: str):
        logging.info(f"renaming: {hdfs_src_path} to: {hdfs_dst_path}")
        self.client.rename(hdfs_src_path=hdfs_src_path, hdfs_dst_path=hdfs_dst_path)
        logging.info(f"Finished renaming: {hdfs_src_path} to: {hdfs_dst_path}")


if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("minimum 5 args are required")
        print("Usage: <operation: file_upload/mkdir/rename> <hdfs url> <hdfs user> <hdfs root dir> <hdfs file path>"
              "<local file path> <overwrite: True/False> (all string)")
    operation = sys.argv[1]
    main(op=operation)
