from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin, BaseUserManager
from django.utils import timezone

# Create your models here.

class Tenants(models.Model):
    class Meta:
        db_table = "tenants"

    id = models.AutoField(primary_key=True)
    tenant_code = models.CharField(max_length=64, verbose_name="Tenant Code", null=False, unique=True)
    tenant_name = models.CharField(max_length=64, verbose_name="Tenant Name", null=False, unique=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class Groups(models.Model):
    class Meta:
        db_table = "groups"

    id = models.AutoField(primary_key=True)
    tenant = models.ForeignKey(Tenants, verbose_name="Tenants Id (Auto Generated)", on_delete=models.CASCADE)
    group_code = models.CharField(max_length=64, verbose_name="Group Code", null=False, unique=True)
    group_name = models.CharField(max_length=64, verbose_name="Group Name", null=False, unique=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class Entities(models.Model):
    class Meta:
        db_table = "entities"

    id = models.AutoField(primary_key=True)
    group = models.ForeignKey(Groups, verbose_name="Groups Id (Auto Generated)", on_delete=models.CASCADE)
    entity_code = models.CharField(max_length=64, verbose_name="Entity Code", null=False, unique=True)
    entity_name = models.CharField(max_length=64, verbose_name="Entity Name", null=False, unique=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class Departments(models.Model):
    class Meta:
        db_table = "departments"

    id = models.AutoField(primary_key=True)
    entity = models.ForeignKey(Entities, verbose_name="Entity Id (Auto Generated", on_delete=models.CASCADE)
    dept_code = models.CharField(max_length=64, verbose_name="Department Code", null=False)
    dept_name = models.CharField(max_length=64, verbose_name="Department Name", null=False)
    status = models.BooleanField(default=True, verbose_name="Status")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class Roles(models.Model):
    class Meta:
        db_table = "roles"

    id = models.AutoField(primary_key=True)
    role_code = models.CharField(max_length=64, verbose_name="Role Code", null=False)
    role_name = models.CharField(max_length=64, verbose_name="Role Name", null=False)
    status = models.BooleanField(default=True, verbose_name="Status")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class Types(models.Model):
    class Meta:
        db_table = "types"

    id = models.AutoField(primary_key=True)
    departments = models.ForeignKey(Departments, verbose_name="Departments Id (Auto Generated)", on_delete=models.CASCADE)
    type = models.CharField(max_length=64, verbose_name="Type", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")

class UserManager(BaseUserManager):

    def create_superuser(self, email, password, username, role, departments, types):
        """
        Creates and saves a User with the given email and password.
        """

        if not email:
            raise ValueError('User must have an email address')

        user = self.model(
            email = self.normalize_email(email),
            username = username,
            role_id = role,
            departments_id = departments,
            types_id = types
        )
        user.set_password(password)
        user.save(using=self._db)
        return user

class Users(AbstractBaseUser, PermissionsMixin):
    class Meta:
        db_table = "users"

    id = models.AutoField(primary_key=True)
    role = models.ForeignKey(Roles, verbose_name="Role Id (Auto Generated)", on_delete=models.CASCADE)
    departments = models.ForeignKey(Departments, verbose_name="Departments Id (Auto Generated)", on_delete=models.CASCADE)
    types = models.ForeignKey(Types, verbose_name="Types Id (Auto Generated)", on_delete=models.CASCADE, null=True)
    email = models.EmailField(max_length=128, unique=True, verbose_name="Email Id")
    username = models.CharField(max_length=64, verbose_name="User Name")
    is_approved = models.BooleanField(default=False, verbose_name="Is Approved ?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField( verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username', 'role', 'departments', 'types']

    objects = UserManager()

class MainModules(models.Model):
    class Meta:
        db_table = "main_modules"

    id = models.AutoField(primary_key=True)
    module_code = models.CharField(max_length=64, verbose_name="Module Code", null=False, unique=True)
    module_name = models.CharField(max_length=64, verbose_name="Module Name", null=False, unique=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField( verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class SubModules(models.Model):
    class Meta:
        db_table = "sub_modules"

    id = models.AutoField(primary_key=True)
    main_module = models.ForeignKey(MainModules, verbose_name="Main Module Id (Auto Generated)", on_delete=models.CASCADE)
    sub_module_code = models.CharField(max_length=64, verbose_name="Sub Module Code", null=False, unique=True)
    sub_module_name = models.CharField(max_length=64, verbose_name="Sub Module Name", null=False, unique=True)
    sub_module_icon = models.CharField(max_length=64, verbose_name="Module Icon", null=True)
    description = models.TextField(verbose_name="Description", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField( verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MainModels(models.Model):
    class Meta:
        db_table = "main_models"

    id = models.AutoField(primary_key=True)
    icon = models.CharField(max_length=128, verbose_name="Icon", null=True, unique=True)
    model_code = models.CharField(max_length=64, verbose_name="Module Code", null=False, unique=True)
    model_name = models.CharField(max_length=64, verbose_name="Module Name", null=False, unique=True)
    menu_icon = models.CharField(max_length=64, verbose_name="Menu Icon", null=True)
    status = models.BooleanField(default=True, verbose_name="Status")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField( verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class SubModels(models.Model):
    class Meta:
        db_table = "sub_models"

    id = models.AutoField(primary_key=True)
    main_model = models.ForeignKey(MainModules, verbose_name="Main Model Id (Auto Generated)", on_delete=models.CASCADE)
    sub_model_code = models.CharField(max_length=64, verbose_name="Sub Module Code", null=False, unique=True)
    sub_model_name = models.CharField(max_length=64, verbose_name="Sub Module Name", null=False)
    sub_menu_icon = models.CharField(max_length=64, verbose_name="Sub Menu Icon", null=True)
    sub_model_status = models.BooleanField(default=True, verbose_name="Sub Module Status")
    url_path = models.CharField(max_length=64, verbose_name="URL Path", null=True, unique=True)
    page_limit = models.PositiveSmallIntegerField(verbose_name="Page Limit", null=False)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField( verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class UserRoles(models.Model):
    class Meta:
        db_table = "user_roles"

    id = models.AutoField(primary_key=True)
    sub_model = models.ForeignKey(SubModels, verbose_name="Sub Model Id (Auto Generated)", on_delete=models.CASCADE)
    sub_modules = models.ForeignKey(SubModules, verbose_name="Sub Module Id (Auto Generated)", on_delete=models.CASCADE)
    role = models.ForeignKey(Roles, verbose_name="Roles Id (Auto Generated)", on_delete=models.CASCADE)
    department = models.ForeignKey(Departments, verbose_name="Department Id (Auto Generated)", on_delete=models.CASCADE)
    tenants = models.ForeignKey(Tenants, verbose_name="Tenants Id (Auto Generated)", on_delete=models.CASCADE)
    groups = models.ForeignKey(Groups, verbose_name="Groups Id (Auto Generated)", on_delete=models.CASCADE)
    entities = models.ForeignKey(Entities, verbose_name="Entities Id (Auto Generated)", on_delete=models.CASCADE)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class MasterUserFilters(models.Model):
    class Meta:
        db_table = "m_user_filters"

    id = models.AutoField(primary_key=True)
    filters = models.CharField(max_length=64, verbose_name="Filters", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class UserFilterAccess(models.Model):
    class Meta:
        db_table = "user_filter_access"

    id = models.AutoField(primary_key=True)
    roles = models.ForeignKey(Roles, verbose_name="Role Id (Auto Generated)", on_delete=models.CASCADE)
    m_user_filters = models.ForeignKey(MasterUserFilters, verbose_name="M User Filters Id (Auto Generated)", on_delete=models.CASCADE)
    is_access = models.BooleanField(default=False, verbose_name="Is Access?")
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class ModuleConnections(models.Model):
    class Meta:
        db_table = "module_connections"

    id = models.AutoField(primary_key=True)
    main_modules = models.ForeignKey(MainModules, verbose_name="Mail Modules Id (Auto Generated)", on_delete=models.CASCADE)
    sub_modules = models.ForeignKey(SubModules, verbose_name="Sub Module Id (Auto Generated)", on_delete=models.CASCADE)
    m_processing_layer_id = models.PositiveIntegerField(verbose_name="Sources - M Processing Layer Id (Auto Generated)", null=True)
    m_processing_sub_layer_id = models.PositiveIntegerField(verbose_name="Sources - M Processing Sub Layer Id (Auto Generated)", null=True)
    is_active = models.BooleanField(default=True, verbose_name="Active ?")
    created_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    created_date = models.DateTimeField(default=timezone.now, verbose_name="Created Date")
    modified_by = models.PositiveSmallIntegerField(verbose_name="User Id", null=True)
    modified_date = models.DateTimeField(default=timezone.now, verbose_name="Modified Date")

class UserLoginLog(models.Model):
    class Meta:
        db_table = "user_login_log"

    id = models.AutoField(primary_key=True)
    users = models.ForeignKey(Users, verbose_name="Users Id (Auto Generated)", on_delete=models.CASCADE)
    system_ip = models.CharField(max_length = 32, verbose_name="System IP Address", null=True)
    login_time = models.DateTimeField(default=timezone.now, verbose_name="Login Time", null=False)
    logout_time = models.DateTimeField(verbose_name="Logout Time", null=True)
