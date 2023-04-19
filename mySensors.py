import paramiko
from airflow.sensors.bash import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

class RemoteFileSensor(BaseSensorOperator):
    """
    Sensor to verify if a file exists on a remote system
    """
    @apply_defaults
    def __init__(self, filepath, hostname, username, password, *args, **kwargs):
        """
        Initilize sensor
        :param filepath: File path to verify if it exists
        :param hostname: Hostname or IP Address of the remote system
        :param username: Username for the SSH connection
        :param password: Password of the SSH connection
        """

        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.hostname = hostname
        self.username = username
        self.password = password

    def poke(self, context):
        """
        Validate if the file exists on the remote server
        :param context: Information about sensor context
        :return: True, if file exists, false if it doesn't
        """

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=self.hostname, username=self.username, password=self.password)
        sftp = ssh.open_sftp()
        try:
            sftp.stat(path=self.filepath)
            self.log.info(f"The file {self.filepath}' exists on the remote server")
            return True
        except IOError:
            self.log.info(f"The file {self.filepath} doesn't exist on the remote sever")
            return False
        finally:
            sftp.close()
            ssh.close()