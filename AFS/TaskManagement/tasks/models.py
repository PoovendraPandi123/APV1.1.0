from django.db import models

# Create your models here.
class Jobs(models.Model):
    class Meta:
        db_table = "jobs"

    jobs_id = models.AutoField(primary_key = True)
    tenants_id = models.PositiveIntegerField(verbose_name="Tenants Id (Business Module - Tenant Id)")
    groups_id = models.PositiveIntegerField(verbose_name="Groups Id (Business Module - Groups Id)")
    entities_id = models.PositiveIntegerField(verbose_name="Entities Id (Business Module - Entities Id)")
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id (Source Module - MProcessingLayerId)")
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id (Source Module - MProcessingSubLayerId)")
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id (Source Module - ProcessingLayerId)")
    job_code = models.CharField(max_length=64, verbose_name="Job Code", null=False)
    job_name = models.TextField(verbose_name="Job Name", null=False)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.job_name

    @property
    def tasks(self):
        return self.tasks_set.all()

    @property
    def actions(self):
        return self.actions_set.all()

    @property
    def job_executions(self):
        return self.jobexecutions_set.all()

    @property
    def execution_sequence(self):
        return self.executionsequence_set.all()

class Tasks(models.Model):
    class Meta:
        db_table = "tasks"

    tasks_id = models.AutoField(primary_key = True)
    jobs = models.ForeignKey(Jobs, verbose_name="Jobs (JobId - Auto Field)", on_delete=models.CASCADE)
    task_code = models.CharField(max_length=64, verbose_name="Task Code", null=False)
    task_name = models.TextField(verbose_name="Task Name", null=False)
    is_system_task = models.BooleanField(default = True, verbose_name="System Task?")
    is_user_task = models.BooleanField(default = True, verbose_name="User Task?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.task_name

    @property
    def actions(self):
        return self.actions_set.all()

    @property
    def execution_sequence(self):
        return self.executionsequence_set.all()

class Actions(models.Model):
    class Meta:
        db_table = "actions"

    actions_id = models.AutoField(primary_key = True)
    jobs = models.ForeignKey(Jobs, verbose_name="Jobs (JobId - Auto Field)", on_delete=models.CASCADE)
    tasks = models.ForeignKey(Tasks, verbose_name="Tasks (TaskId - Auto Field)", on_delete=models.CASCADE)
    source_id = models.PositiveIntegerField(verbose_name="Source Id (Sources - Auto Field)", null=True)
    action_code = models.CharField(max_length=64, verbose_name="Action Code", null=False)
    action_name = models.TextField(verbose_name="Action Name", null=False)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

    def __str__(self):
        return self.action_name

    @property
    def execution_sequence(self):
        return self.executionsequence_set.all()

class ExecutionSequence(models.Model):
    class Meta:
        db_table = "execution_sequence"

    exc_seq_id = models.AutoField(primary_key = True)
    jobs = models.ForeignKey(Jobs, verbose_name="Jobs (JobId - Auto Field)", on_delete=models.CASCADE)
    tasks = models.ForeignKey(Tasks, verbose_name="Tasks (TaskId - Auto Field)", on_delete=models.CASCADE)
    actions = models.ForeignKey(Actions, verbose_name="Actions (ActionsId - Auto Field)", on_delete=models.CASCADE)
    sequence = models.PositiveIntegerField(verbose_name="Sequence", null=False)
    sub_sequence = models.PositiveIntegerField(verbose_name="Sub Sequence", null=False)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(auto_now_add=True, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(auto_now=True, verbose_name="Modified Date")

class JobExecutions(models.Model):
    class Meta:
        db_table = "job_executions"

    job_execution_id = models.AutoField(primary_key = True)
    file_ids = models.JSONField(verbose_name="File Id (Process Module - File Uploads - File Id (Auto Field)", null=False)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="M Processing Layer Id - Sources - M Processing Layer Id (Auto Field)", null=False)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="M Processing Sub Layer Id - Sources - M Processing Sub Layer Id (Auto Field)", null=False)
    processing_layer_id = models.PositiveIntegerField(verbose_name="Processing Layer Id - Sources - Processing Layer Id (Auto Field)", null=False)
    execution_status = models.CharField(max_length=256, verbose_name="Execution Status")
    start_dt = models.DateTimeField(verbose_name="Start Date Time", null=True)
    end_dt = models.DateTimeField(verbose_name="End Date Time", null=True)
    duration = models.PositiveIntegerField(verbose_name="Task Duration", null=True)
    executed_by = models.PositiveIntegerField(verbose_name="Executed By", null=True)
    updated_by = models.PositiveIntegerField(verbose_name="Updated By", null=True)

    def __str__(self):
        return self.execution_status





