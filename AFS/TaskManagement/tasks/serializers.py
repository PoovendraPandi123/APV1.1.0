from rest_framework import serializers
from .models import *

class ExecutionSequenceSerializer(serializers.ModelSerializer):
    exc_seq_id = serializers.IntegerField(required=False)
    class Meta:
        model = ExecutionSequence
        fields = ['exc_seq_id', 'job', 'task', 'sequence', 'sub_sequence', 'is_active', 'created_by', 'modified_by']
        read_onl_fields = ('job', 'task',)

class TaskSerializer(serializers.ModelSerializer):
    task_id = serializers.IntegerField(required=False)
    class Meta:
        model = Tasks
        fields = ['task_id', 'job', 'task_name', 'is_system_task', 'is_user_task', 'is_active', 'created_by', 'modified_by']
        read_only_fields = ('job', )

class JobSerializer(serializers.ModelSerializer):
    tasks = TaskSerializer(many=True)
    execution_sequence = ExecutionSequenceSerializer(many=True)
    class Meta:
        model = Jobs
        fields = ['job_id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'job_name', 'is_active', 'created_by', 'modified_by', 'tasks', 'execution_sequence']

class JobExecutionSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobExecutions
        fields = ['job_execution_id', 'execution_status', 'start_dt', 'end_dt', 'duration', 'executed_by', 'updated_by', 'job_id']