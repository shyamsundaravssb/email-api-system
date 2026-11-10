FROM python:3.12-slim

RUN pip install --no-cache-dir aiosmtpd

COPY server.py /app/server.py

WORKDIR /app

CMD ["python", "server.py"]



