FROM python:3.11-slim

WORKDIR /opt/project
RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.34.0 kafka-python==2.0.2
ENV PYTHONPATH=/opt/project/src PROJECT_ROOT=/opt/project
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
