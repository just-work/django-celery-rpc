from rest_framework import serializers
from models import SimpleModel


class SimpleTaskSerializer(serializers.ModelSerializer):
    """
    Test serializer
    """
    class Meta:
        model = SimpleModel
        fields = ('id', )

