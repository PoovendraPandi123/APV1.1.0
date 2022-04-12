from rest_framework import serializers
from .models import *

class VendorMasterSerializer(serializers.ModelSerializer):
    class Meta:
        model = VendorMaster
        fields = '__all__'

class ReconFileUploadsSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconFileUploads
        fields = '__all__'