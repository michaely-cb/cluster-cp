import os


def init_orm():
    import django
    from django.conf import settings

    if settings.configured:
        return

    settings.configure(
        INSTALLED_APPS=[
            'deployment_manager.db',
        ],
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': f'{os.getenv("CLUSTER_DEPLOYMENT_BASE", ".")}/dm.db'
            }
        },
        USE_TZ=True,
    )
    django.setup()
