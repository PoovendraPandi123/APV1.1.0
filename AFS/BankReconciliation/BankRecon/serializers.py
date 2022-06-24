from rest_framework import serializers
from .models import *

class ReconFileUploadsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconFileUploads
        fields = '__all__'

class MasterMatchingCommentsSerializer(serializers.ModelSerializer):
    class Meta:
        model = MasterMatchingComments
        fields = '__all__'