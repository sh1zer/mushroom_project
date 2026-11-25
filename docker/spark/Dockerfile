FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root
RUN pip install --no-cache-dir numpy==1.26.4 pandas==2.2.3 pyarrow==17.0.0
ENV PYTHONPATH=/opt/project/src PROJECT_ROOT=/opt/project
WORKDIR /opt/project
