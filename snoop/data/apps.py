from django.apps import AppConfig
from django.contrib.admin.apps import AdminConfig


class DataConfig(AppConfig):
    name = 'data'


class AdminConfig(AdminConfig):
    default_site = 'snoop.data.admin.SnoopAdminSite'
