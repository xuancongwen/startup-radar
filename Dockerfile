# One image, two commands: the pipeline (default) and the read-only API (startup-radar-serve).
FROM python:3.12-slim
RUN useradd --uid 1000 --user-group --create-home --shell /usr/sbin/nologin radar \
    && mkdir /data && chown radar:radar /data
WORKDIR /app
COPY pyproject.toml ./
COPY startup_radar ./startup_radar
RUN pip install --no-cache-dir . && rm -rf /root/.cache
USER radar
VOLUME ["/data"]
ENV PYTHONUNBUFFERED=1
CMD ["startup-radar", "--db", "/data/radar.sqlite3", "--output", "/data/shortlists"]
