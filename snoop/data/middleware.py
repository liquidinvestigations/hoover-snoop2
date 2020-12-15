from django.contrib.auth.models import User


class AutoLogin():
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
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        setattr(request, '_dont_enforce_csrf_checks', True)
        return self.get_response(request)
