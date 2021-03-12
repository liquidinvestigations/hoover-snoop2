"""Django middlewares for removing framework functionality.

Enforces system-wade disabling of the Login System (automatically logging everyone in as an admin with no
password visiting the page) and the CSRF protection system (that requires headers from this server to reach
the UI and back.

As stated in the readme, this Django instance leaves access control and security a problem outside its
scope. The port needs to be firewalled off and made accessible only to sysadmins debugging the Tasks table.
"""

from django.contrib.auth.models import User


class AutoLogin():
    """Middleware that automatically logs anonymous users in as an administrator named "root".

    Since the Django Admin can't work without the concept of users, we couldn't disable the system
    - so we use this middleware to create and log in an admin user called "root", and use the admin
    normally.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        root, created = User.objects.get_or_create(username='root')
        if created or not root.is_staff:
            root.is_staff = True
            root.is_superuser = True
            root.is_admin = True
            root.is_active = True
            root.set_password('root')
            root.save()

        request.user = root
        return self.get_response(request)


class DisableCSRF():
    """Middleware that patches requests to disable CSRF checks.

    Since the Django Admin can't work without CSRF enabled, we couldn't disable the system
    - so we use this middleware to patch it out.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        setattr(request, '_dont_enforce_csrf_checks', True)
        return self.get_response(request)
