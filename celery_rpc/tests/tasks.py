from celery_rpc.base import ModelTask


# For testing override base task class
class CustomModelTask(ModelTask):
    def test_marker(self):
        pass