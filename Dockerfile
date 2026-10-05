FROM python:3.12 AS builder

# The lv_* connectors live in private Lab-Work repos, so they can't come from
# requirements.txt alone. GITHUB_TOKEN arrives as a build arg (in CI, Drone
# passes its GITHUB_PUSH_TOKEN secret); installing in this builder stage keeps
# the token out of the final image's layer history.
ARG GITHUB_TOKEN
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# https://x-access-token:${GITHUB_TOKEN}@
# RUN pip install --no-cache-dir \
#     git+github.com/Lab-Work/lv_telemetry_connector \   
#     git+github.com/Lab-Work/lv_kafka_connector \
#     git+github.com/Lab-Work/lv_db_connector

COPY ./lv_telemetry_connector ./lv_telemetry_connector
COPY ./lv_kafka_connector ./lv_kafka_connector
COPY ./lv_db_connector ./lv_db_connector
COPY requirements.txt .
RUN ls -la
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.12

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY . .

# JSON logs go to stdout; Prometheus metrics are served on :9100 (and :9101,
# :9102 ... for workers 1, 2 ... when WORKERS > 1). Exec-form CMD so SIGTERM
# reaches Python and each worker can finish its batch before exiting.
EXPOSE 9100
CMD ["python3", "ouster2db.py"]
