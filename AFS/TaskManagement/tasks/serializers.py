from rest_framework import serializers
from .models import *

class ExecutionSequenceSerializer(serializers.ModelSerializer):
    exc_seq_id = serializers.IntegerField(required=False)
    class Meta:
        model = ExecutionSequence
        fields = ['exc_seq_id', 'jobs', 'tasks', 'actions', 'sequence', 'sub_sequence', 'is_active', 'created_by', 'modified_by']
        read_only_fields = ('jobs', 'tasks', 'actions')

class ActionSerializer(serializers.ModelSerializer):
    actions_id = serializers.IntegerField(required=False)
    class Meta:
        models = Actions
        fields = ['actions_id', 'jobs', 'tasks', 'action_code', 'action_name', 'is_active', 'created_by', 'modified_by']
        read_only_fields = ('jobs', 'tasks', )

class TaskSerializer(serializers.ModelSerializer):
    task_id = serializers.IntegerField(required=False)
    class Meta:
        model = Tasks
        fields = ['tasks_id', 'jobs', 'task_code', 'task_name', 'is_system_task', 'is_user_task', 'is_active', 'created_by', 'modified_by']
        read_only_fields = ('job', )

class JobSerializer(serializers.ModelSerializer):
    tasks = TaskSerializer(many=True)
    actions = ActionSerializer(many=True)
    execution_sequence = ExecutionSequenceSerializer(many=True)
    class Meta:
        model = Jobs
        fields = ['jobs_id', 'tenants_id', 'groups_id', 'entities_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'job_code', 'job_name', 'is_active', 'created_by', 'modified_by', 'tasks', 'execution_sequence']

class JobExecutionSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobExecutions
        fields = ['job_execution_id', 'file_id', 'm_processing_layer_id', 'm_processing_sub_layer_id', 'processing_layer_id', 'execution_status', 'start_dt', 'end_dt', 'duration', 'executed_by', 'updated_by']