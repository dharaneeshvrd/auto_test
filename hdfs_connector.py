from pywebhdfs.webhdfs import PyWebHdfsClient


class HDFS(object):
    def __init__(self, host, port, user):
        self._hdfs = PyWebHdfsClient(
            host=host, port=port, user_name=user, timeout=None)
        logging.debug('webhdfs = %s@%s:%s', user, host, port)

    def file_exists(self, path):

        try:
            self._hdfs.get_file_dir_status(path)
            return True
        except:
            return False
