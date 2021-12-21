from .models import *
from django.http import JsonResponse
import json
from django.contrib.auth import authenticate
import logging
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

# Create your views here.

logger = logging.getLogger("business_modules_one")

@csrf_exempt
def get_user_rights(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)
            user_name = ''
            for k,v in data.items():
                if k == "user_name":
                    user_name = v
            if len(str(user_name)) > 0:
                role_id=''
                users = Users.objects.filter(username=user_name)
                for user in users:
                    role_id = user.role_id

                modules_id_list=UserRoles.objects.filter(role=role_id).order_by().values_list('sub_modules').distinct()
                m_sub_modules_list=SubModules.objects.filter(id__in=modules_id_list)
                modules_list=[]
                for m_module in m_sub_modules_list:
                    modules_list.append({"module_id": m_module.id,
                                         "module_name": m_module.sub_module_name,
                                         "module_icon" : m_module.sub_module_icon,
                                         "module_description" : m_module.description})

                return JsonResponse({"Status": "Success", "modules_list": modules_list, "role_id": role_id})
            else:
                return JsonResponse({"Status": "Error", "Message": "Username not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method not Found!!!"})
    except Exception as e:
        logger.error(str(e))
        logger.error("Error in Login User", exc_info=True)
        return JsonResponse({"Status" : "Error"})

@csrf_exempt
def get_user_login(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            user_name = ''
            password = ''
            login_time = ''
            system_ip = ''

            for k,v in data.items():
                if k == "username":
                    user_name = v
                if k == "password":
                    password = v
                if k == "login_time":
                    login_time = v
                if k == "system_ip":
                    system_ip = v

            if len(str(user_name)) > 0:
                if len(str(password)) > 0:
                    if len(str(login_time)) > 0:
                        if len(str(system_ip)) > 0:
                            user = authenticate(request, username = user_name, password = password)
                            if user:
                                users = Users.objects.filter(email=user_name)
                                for user in users:
                                    user_id = user.id
                                    user_name_login = user.username
                                    department_id = user.departments_id

                                departments = Departments.objects.filter(id=department_id)
                                for department in departments:
                                    user_department = department.dept_name

                                UserLoginLog.objects.create(
                                    system_ip = system_ip,
                                    login_time = timezone.now(),
                                    users_id = user_id
                                )
                                return JsonResponse({"Status": "Success", "user_id": user_id, "user_name": user_name_login, "department": user_department, "system_ip" : system_ip})
                            else:
                                return JsonResponse({"Status" : "Error", "Message" : "User Not Found!!!"})
                        else:
                            return JsonResponse({"Status" : "Error", "Message" : "System IP not Found!!!"})
                    else:
                        return JsonResponse({"Status" : "Error", "Message" : "Login Time not Found!!!"})
                else:
                    return JsonResponse({"Status" : "Error", "Message" : "Password not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error", "Message" : "Username not Found!!!"})

    except Exception as e:
        logger.error(str(e))
        logger.error("Error in Login User", exc_info=True)
        return JsonResponse({"Status" : "Error"})

@csrf_exempt
def get_user_logout(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            system_ip = ''
            user_id = ''

            for k,v in data.items():
                if k == "system_ip":
                    system_ip = v
                if k == "user_id":
                    user_id = v

            if len(str(system_ip)) > 0:
                if len(str(user_id)) > 0:
                    UserLoginLog.objects.create(
                        system_ip=system_ip,
                        logout_time=timezone.now(),
                        users_id=user_id
                    )
                    return JsonResponse({"Status" : "Success"})
                else:
                    return JsonResponse({"Status" : "Error", "Message" : "User Id Not Found!!!"})
            else:
                return JsonResponse({"Status" : "Error", "Message" : "System IP Not Found!!!"})
        else:
            return JsonResponse({"Status" : "Error", "Message" : "POST Method not Received!!!"})
    except Exception as e:
        logger.error(str(e))
        logger.error("Error in Logout User", exc_info=True)
        return JsonResponse({"Status" : "Error"})


def get_tenant():
    try:
        tenants_list = []
        tenants = Tenants.objects.all()
        for tenant in tenants:
            tenants_list.append({
                "id": tenant.id,
                "tenant_name": tenant.tenant_name
            })
        return tenants_list
    except Exception:
        logger.error("Error in Getting Tenant!!!", exc_info=True)
        return {"Status": "Error"}

def get_groups(tenant_id):
    try:
        groups_list = []
        groups = Groups.objects.filter(tenant_id=tenant_id)
        for group in groups:
            groups_list.append({
                "id": group.id,
                "group_name": group.group_name,
                "tenant_id": group.tenant_id
            })
        return groups_list
    except Exception:
        logger.error("Error in Getting Group!!!", exc_info=True)
        return {"Status": "Error"}

def get_entities(group_id):
    try:
        entities_list = []
        entities = Entities.objects.filter(group_id=group_id)
        for entity in entities:
            entities_list.append({
                "id": entity.id,
                "entity_name": entity.entity_name,
                "group_id": entity.group_id
            })
        return  entities_list
    except Exception:
        logger.error("Error in Getting Entities!!!", exc_info=True)
        return {"Status": "Error"}

def get_departments(entity_id):
    try:
        department_list = []
        departments = Departments.objects.filter(entity_id=entity_id)
        for department in departments:
            department_list.append({
                "id": department.id,
                "dept_name": department.dept_name,
                "entity_id": department.entity_id
            })
        return  department_list
    except Exception:
        logger.error("Error in Getting Departments!!!", exc_info=True)
        return {"Status": "Error"}

def get_types(departments_id):
    try:
        types_list = []
        types = Types.objects.filter(departments_id=departments_id)
        for type in types:
            types_list.append({
                "id": type.id,
                "type": type.type
            })
        return types_list
    except Exception:
        logger.error("Error in Getting Types!!!", exc_info=True)
        return {"Status": "Error"}

def get_default_tenant(tenant_id):
    try:
        tenant_filter = dict()
        tenants = Tenants.objects.filter(id = tenant_id)
        for tenant in tenants:
            tenant_filter["id"] = tenant.id
            tenant_filter["value"] = tenant.tenant_name
        return tenant_filter
    except Exception:
        logger.error("Error in Getting Default Tenant!!!", exc_info=True)
        return {"Status": "Error"}

def get_default_group(group_id):
    try:
        group_filter = dict()
        groups = Groups.objects.filter(id = group_id)
        for group in groups:
            group_filter["id"] = group.id
            group_filter["value"] = group.group_name
        return group_filter
    except Exception:
        logger.error("Error in Getting Default Group!!!", exc_info=True)
        return {"Status": "Error"}

def get_default_entity(entity_id):
    try:
        entity_filter = dict()
        entities = Entities.objects.filter(id = entity_id)
        for entity in entities:
            entity_filter["id"] = entity.id
            entity_filter["value"] = entity.entity_name
        return entity_filter
    except Exception:
        logger.error("Error in Getting Default Entity!!!", exc_info=True)
        return {"Status": "Error"}

def get_default_department(department_id):
    try:
        department_filter = dict()
        departments = Departments.objects.filter(id = department_id)
        for department in departments:
            department_filter["id"] = department.id
            department_filter["value"] = department.dept_name
        return department_filter
    except Exception:
        logger.error("Error in Getting Default Department!!!", exc_info=True)
        return {"Status": "Error"}

def get_default_type(type_id):
    try:
        type_filter = dict()
        types = Types.objects.filter(id = type_id)
        for type in types:
            type_filter["id"] = type.id
            type_filter["value"] = type.type
        return type_filter
    except Exception:
        logger.error("Error in Getting Default Type!!!", exc_info=True)
        return {"Status": "Error"}

@csrf_exempt
def get_user_credentials(request, *args, **kwargs):
    try:
        if request.method == "POST":
            body = request.body.decode('utf-8')
            data = json.loads(body)

            user_id = ''
            sub_module_id = ''

            for k,v in data.items():
                if k == "user_id":
                    user_id = v
                if k == "module_id":
                    sub_module_id = v

            if len(str(user_id)) > 0:
                # Get the role for the user

                users = Users.objects.filter(id = user_id, is_active = True)

                for user in users:
                    role_id = user.role_id
                    types_id = user.types_id
                    departments_id = user.departments_id

                user_role = UserRoles.objects.filter(role_id = role_id, is_active = True, sub_modules = sub_module_id)
                tenants_ids = []
                groups_ids = []
                entities_ids = []
                sub_model_ids = []
                module_ids = []

                for role in user_role:
                    tenants_ids.append(role.tenants_id)
                    groups_ids.append(role.groups_id)
                    entities_ids.append(role.entities_id)
                    sub_model_ids.append(role.sub_model_id)
                    module_ids.append(role.sub_modules_id)

                # Getting Unique of Tenant, group and entity id
                tenants_id_unique = list(set(tenants_ids))[0]
                groups_id_unique = list(set(groups_ids))[0]
                entities_id_unique = list(set(entities_ids))[0]
                module_ids_unique = list(set(module_ids))[0]

                # List of Ids in Sub Models
                sub_model_ids_unique = list(set(sub_model_ids))

                sub_models_list = []
                main_model_ids = []

                # Creating SubModels List
                for sub_model_id in sub_model_ids_unique:
                    sub_models = SubModels.objects.filter(id = sub_model_id, is_active = True)

                    for sub_model in sub_models:
                        sub_model_name = sub_model.sub_model_name
                        url_path = sub_model.url_path
                        menu_icon = sub_model.sub_menu_icon
                        page_limit = sub_model.page_limit
                        main_model_id = sub_model.main_model_id
                        main_model_ids.append(sub_model.main_model_id) # Getting Main Model ids

                    sub_model_dict = {
                        "sub_model_id" : sub_model_id,
                        "main_model_id" : main_model_id,
                        "sub_model_name" : sub_model_name,
                        "menu_icon" : menu_icon,
                        "url_path" : url_path,
                        "page_limit" : page_limit
                    }
                    sub_models_list.append(sub_model_dict)

                # Creating Main Models List
                main_model_ids_unique = list(set(main_model_ids))
                main_models_list = []

                for main_model_id in main_model_ids_unique:
                    main_models = MainModels.objects.filter(id = main_model_id, is_active = True)

                    for main_model in main_models:
                        model_name = main_model.model_name
                        menu_icon = main_model.menu_icon

                    main_model_dict = {
                        "main_model_id" : main_model_id,
                        "module_name" : model_name,
                        "menu_icon": menu_icon,
                    }

                    main_models_list.append(main_model_dict)

                tenants = Tenants.objects.filter(id = tenants_id_unique)

                for tenant in tenants:
                    tenant_code = tenant.tenant_code

                tenant_extension = tenant_code.split("@")[-1]

                module_connections = ModuleConnections.objects.filter(
                    sub_modules_id = sub_module_id
                )

                for connection in module_connections:
                    m_processing_layer_id = connection.m_processing_layer_id
                    m_processing_sub_layer_id = connection.m_processing_sub_layer_id

                user_filter_access = UserFilterAccess.objects.filter(roles_id = role_id, is_access = 1)

                m_user_filters_ids = []
                for access in user_filter_access:
                    m_user_filters_ids.append(access.m_user_filters_id)

                # print(m_user_filters_ids)

                filter_access = dict()
                default_filters = {
                    "default_tenant": {},
                    "default_group": {},
                    "default_entity": {},
                    "default_department" : {},
                    "default_type": {}
                }

                if 1 in m_user_filters_ids:
                    filter_access["tenant_filter"] = "No Access" # Tenant Filter
                    filter_access["group_filter"] = get_groups(tenants_id_unique) # Group Filter
                    filter_access["entity_filter"] = get_entities(groups_id_unique) # Entity Filter
                    filter_access["department_filter"] = get_departments(entities_id_unique) # Department Filter
                    filter_access["type_filter"] = get_types(departments_id) # Type Filter

                    default_filters["default_tenant"] = get_default_tenant(tenants_id_unique) # Default Tenant

                elif 2 in m_user_filters_ids:
                    filter_access["tenant_filter"] = "No Access" # Tenant Filter
                    filter_access["group_filter"] = "No Access"  # Group Filter
                    filter_access["entity_filter"] = get_entities(groups_id_unique)  # Entity Filter
                    filter_access["department_filter"] = get_departments(entities_id_unique)  # Department Filter
                    filter_access["type_filter"] = get_types(departments_id)  # Type Filter

                    default_filters["default_tenant"] = get_default_tenant(tenants_id_unique) # Default Tenant
                    default_filters["default_group"] = get_default_group(groups_id_unique) # Default Group

                elif 3 in m_user_filters_ids:
                    filter_access["tenant_filter"] = "No Access" # Tenant Filter
                    filter_access["group_filter"] = "No Access"  # Group Filter
                    filter_access["entity_filter"] = "No Access"  # Entity Filter
                    filter_access["department_filter"] = get_departments(entities_id_unique)  # Department Filter
                    filter_access["type_filter"] = get_types(departments_id)  # Type Filter

                    default_filters["default_tenant"] = get_default_tenant(tenants_id_unique) # Default Tenant
                    default_filters["default_group"] = get_default_group(groups_id_unique) # Default Group
                    default_filters["default_entity"] = get_default_entity(entities_id_unique) # Default Entity

                elif 4 in m_user_filters_ids:
                    filter_access["tenant_filter"] = "No Access" # Tenant Filter
                    filter_access["group_filter"] = "No Access"  # Group Filter
                    filter_access["entity_filter"] = "No Access"  # Entity Filter
                    filter_access["department_filter"] = "No Access"  # Department Filter
                    filter_access["type_filter"] = get_types(departments_id)  # Type Filter

                    default_filters["default_tenant"] = get_default_tenant(tenants_id_unique) # Default Tenant
                    default_filters["default_group"] = get_default_group(groups_id_unique) # Default Group
                    default_filters["default_entity"] = get_default_entity(entities_id_unique) # Default Entity
                    default_filters["default_department"] = get_default_department(departments_id) # Default Department

                elif 5 in m_user_filters_ids:
                    filter_access["tenant_filter"] = "No Access" # Tenant Filter
                    filter_access["group_filter"] = "No Access"  # Group Filter
                    filter_access["entity_filter"] = "No Access"  # Entity Filter
                    filter_access["department_filter"] = "No Access"  # Department Filter
                    filter_access["type_filter"] = "No Access"  # Type Filter

                    default_filters["default_tenant"] = get_default_tenant(tenants_id_unique) # Default Tenant
                    default_filters["default_group"] = get_default_group(groups_id_unique) # Default Group
                    default_filters["default_entity"] = get_default_entity(entities_id_unique) # Default Entity
                    default_filters["default_department"] = get_default_department(departments_id) # Default Department
                    default_filters["default_type"] = get_default_type(types_id) # Default Type

                elif 6 in m_user_filters_ids:
                    filter_access["tenant_filter"] = get_tenant() # Tenant Filter
                    filter_access["group_filter"] = get_groups(tenants_id_unique) # Group Filter
                    filter_access["entity_filter"] = get_entities(groups_id_unique) # Entity Filter
                    filter_access["department_filter"] = get_departments(entities_id_unique) # Department Filter
                    filter_access["type_filter"] = get_types(departments_id) # Type Filter

                user_type = Users.objects.filter(types_id = types_id)

                if user_type:
                    for _type in user_type:
                        type_id = _type.types_id

                if type_id is None:
                    type_id = 0

                # print(filter_access)
                return JsonResponse({
                    "Status": "Success",
                    "main_models" : main_models_list,
                    "sub_models" : sub_models_list,
                    "tenant_id" : tenants_id_unique,
                    "group_id" : groups_id_unique,
                    "entity_id" : entities_id_unique,
                    "module_id" : module_ids_unique,
                    "tenant_extension" : tenant_extension,
                    "m_processing_layer_id": m_processing_layer_id,
                    "m_processing_sub_layer_id": m_processing_sub_layer_id,
                    "filter_access": filter_access,
                    "default_filters": default_filters,
                    "type_id": type_id
                })
            else:
                return JsonResponse({"Status" : "Error", "Message" : "User Id Not Found!!!"})

    except Exception as e:
        logger.error(str(e))
        logger.error("Error in Getting User Credentials", exc_info=True)
        return JsonResponse({"Status": "Error"})