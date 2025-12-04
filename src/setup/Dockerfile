FROM confluentinc/cp-kafka:latest

USER root
COPY create-topics.sh /docker-entrypoint-init.d/create-topics.sh
RUN chmod +x /docker-entrypoint-init.d/create-topics.sh

USER appuser
