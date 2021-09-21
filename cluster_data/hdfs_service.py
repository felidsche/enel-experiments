import sys

from hdfs import InsecureClient


class HdfsService():
    def __init__(self, client):
        self.client = client

    def write_file_to_hdfs(self, file_path, hdfs_dir):
        with open(file_path) as reader, self.client.write(hdfs_dir + file_path) as writer:
            for line in reader:
                if line.startswith('-'):
                    writer.write(line)


if __name__ == '__main__':
    if len(sys.argv) < 6:
        print("Usage: <hdfs url> <hdfs user> <hdfs root dir> <local file path> <hdfs dir from root dir> (all string)")
    url = sys.argv[1]
    user = sys.argv[2]
    root = sys.argv[3]
    file_path = sys.argv[4]
    hdfs_dir = sys.argv[5]
    client = InsecureClient(url=url, user=user, root=root)
    hdfs_service = HdfsService(client=client)
    hdfs_service.write_file_to_hdfs(file_path=file_path, hdfs_dir=hdfs_dir)
