FROM python:3.13-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    LANG=C.UTF-8 \
    APP_HOME=/usr/src/monapps \
    DJANGO_SETTINGS_MODULE=monapps.settings \
    DEV_MODE=1


WORKDIR $APP_HOME

ARG GID=1000
ARG UID=1000
ARG USER=monapps

RUN  apk add --update --no-cache curl && \
    pip install --upgrade pip setuptools && \
    addgroup -g $GID -S $USER && \
    adduser -S $USER -G $USER --uid "$UID"

COPY ./docker/dev/monapps/requirements.txt  /
RUN pip install -r /requirements.txt

COPY --chown=$USER:$USER ./docker/dev/monapps/entrypoints /
COPY --chown=$USER:$USER ./monapps $APP_HOME

RUN chmod +x /*.sh && \
    mkdir -p $APP_HOME/static && \
    chown -R $USER:$USER $APP_HOME && \
    chown -R $USER:$USER /usr/src

ENTRYPOINT ["/entrypoint.sh"]

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000", "--noreload"]

EXPOSE 8000

USER $USER
