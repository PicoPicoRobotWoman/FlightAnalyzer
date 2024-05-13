FROM spark-base:latest

COPY spark-submit.sh /

ENV SPARK_MASTER_URL="spark://spark-master:7077" \
    SPARK_SUBMIT_ARGS="" \
    SPARK_APPLICATION_ARGS=""

CMD ["/bin/bash", "/spark-submit.sh"]
