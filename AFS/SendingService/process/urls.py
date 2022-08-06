from django.urls import path
from .views import *

urlpatterns = [
    path('get_read_mail_from_outlook/', get_read_mail_from_outlook, name="get_read_mail_from_outlook"),
    path('get_send_mail_to_vendor_through_outlook/', get_send_mail_to_vendor_through_outlook, name="get_send_mail_to_vendor_through_outlook")
]