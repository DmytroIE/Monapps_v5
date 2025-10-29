#!/bin/sh

python manage.py wait_for_db

# https://stackoverflow.com/questions/37836764/run-command-in-docker-container-only-on-the-first-start
CONTAINER_ALREADY_STARTED="/usr/src/already_started.txt"
if [ -e "$CONTAINER_ALREADY_STARTED" ]; then
    echo "Not the first start"
else
    echo "Start for the first time"
    echo "Running migrations and creating superuser"
    touch $CONTAINER_ALREADY_STARTED

    python manage.py migrate

    # https://stackoverflow.com/questions/30027203/create-django-super-user-in-a-docker-container-without-inputting-password

    if [ "$DJANGO_SUPERUSER_USERNAME" ]
    then
        python manage.py createsuperuser \
            --noinput \
            --username $DJANGO_SUPERUSER_USERNAME \
            --email $DJANGO_SUPERUSER_EMAIL
    fi
    python manage.py loaddata fixtures.json
fi

exec "$@"
