FROM spark-base:latest

COPY start-worker.sh /

ENV SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_LOG=/spark/logs \
    SPARK_MASTER="spark://spark-master:7077"

EXPOSE 8081

CMD ["/bin/bash", "/start-worker.sh"]
