from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SSHSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(SSHSensor, self).__init__(*args, **kwargs)

    def pke(self, context):