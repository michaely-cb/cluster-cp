import pathlib
import sqlite3
import sys


def requires_migration(db_path: str) -> bool:
    """ Safety check - if the first migration is 0001_baseline, then we've already applied this migration. Re-applying
    could cause data loss.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("select name from django_migrations where id = 1;")
        v = cur.fetchone()
        if v is None or v != "0001_baseline":
            return True
        return False


def post_django_migrate(db_path: str):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # Reset the state of the migrations table to an initial migration state
        cur.execute("DELETE FROM django_migrations;")
        cur.execute("UPDATE sqlite_sequence SET seq = 0 WHERE name = 'django_migrations'")
        cur.execute("""
            INSERT INTO django_migrations (app, name, applied) 
            VALUES ('db', '0001_baseline', datetime('now'))
        """)
        conn.commit()


def do_auto_to_managed_migrations_migration(db_file: str) -> bool:
    db_file = pathlib.Path(db_file)
    if not db_file.is_file():
        return False
    db_file = str(db_file.absolute())

    if not requires_migration(db_file):
        return False

    import django.db
    from django.conf import settings
    from django.core.management import call_command

    settings.configure(
        INSTALLED_APPS=[
            'schema_migrator.db',
        ],
        DATABASES={
            'default': {
                'ENGINE': 'django.db.backends.sqlite3',
                'NAME': db_file,
            }
        }
    )
    print("Migrating to use file-based migration strategy")
    django.setup()
    call_command('makemigrations', 'db')
    call_command('migrate')

    print("Re-initializing migrations table")
    post_django_migrate(db_file)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError("expected 1 argument: DB_FILENAME")
    new_path = []
    for p in sys.path:
        # remove from path else migrations in deployment_manager will be discovered
        if p != '/opt/cerebras/cluster-deployment/deployment':
            new_path.append(p)
    sys.path = new_path
    do_auto_to_managed_migrations_migration(sys.argv[1])
