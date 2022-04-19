from django.urls import path
from . import views

urlpatterns = [
    # path('update_source/', views.get_update_source, name="get_update_source"),
    path('get_insert_source/', views.get_insert_source, name="get_insert_source"),
    path('source_list/', views.get_source_list_details, name="source_list"),
    path('upload_files/', views.upload_files, name="upload_files"),
    path('source_field_list/', views.get_source_field_list, name="get_source_field_list"),
    path('source_field_list_details/', views.get_field_list_details, name="get_field_list_details"),
    path('source_definitions_list/',views.get_source_def_list_details, name="source_definitions_list"),
    path('aggregator_details/', views.get_aggregator_details, name="aggregator_details"),
    path('aggregator_list_details/', views.get_aggregator_list_details, name="aggregator_list_details"),
    path('aggregator_list/', views.get_aggregators_list_details, name="aggregator_list"),
    path('transformation_source/', views.get_transformation_source, name="transformation_source"),
    path('transformation_page/', views.get_transformation_page, name="transformation_page"),
    path('transformations_fields/', views.get_transformations_fields, name="transformations_fields"),
    path('transformations_field_extraction/', views.get_transformations_field_extraction, name="transformations_field_extraction"),
    path('processing_layers/', views.get_processing_layers, name="processing_layers"),
    path('processing_sub_layers/', views.get_processing_sub_layers, name="processing_sub_layers"),
    path('update_processing_layer/', views.get_update_processing_layer, name="update_processing_layer"),
    path('business_layer_source/', views.get_business_layer_source, name="business_layer_source"),
    path('update_business_layer/', views.get_update_business_layer, name="update_business_layer"),
    path('bus_lay_process_def/', views.get_bus_lay_process_def, name="bus_lay_process_def"),
    path('bus_lay_rule_set/', views.get_bus_lay_rule_set, name="bus_lay_rule_set"),
    path('bus_lay_rule_name/', views.get_bus_lay_rule_name, name="bus_lay_rule_name"),
    path('get_processing_layer_list/', views.get_processing_layer_list, name="get_processing_layer"),
    path('get_processing_layer_def_list/', views.get_processing_layer_def_list, name="get_processing_layer_def_list"),
    path('business_layer_definition/', views.business_layer_definition, name="update_business_layer"),
    path('get_source_insert_queries/', views.get_source_insert_queries, name="get_source_insert_queries"),
    path('get_database_values/', views.get_database_values, name="get_database_values"),
    path('get_check_spark/', views.get_check_spark, name="get_check_spark")
]

# 26AS
urlpatterns += [
    path('get26AS_processing_layer_def_list/', views.get26AS_processing_layer_def_list, name="get26AS_processing_layer_def_list")
]

# Medical Insurance
urlpatterns += [
    path('get_med_ins_processing_layer_def_list/', views.get_med_ins_processing_layer_def_list, name="get_med_ins_processing_layer_def_list")
]