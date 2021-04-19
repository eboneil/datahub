#!/bin/bash
: ${PARTITIONS:=1}
: ${REPLICATION_FACTOR:=1}

: ${KAFKA_PROPERTIES_SECURITY_PROTOCOL:=PLAINTEXT}

CONNECTION_PROPERTIES_PATH=/tmp/connection.properties

echo "bootstrap.servers=$KAFKA_BOOTSTRAP_SERVER" > $CONNECTION_PROPERTIES_PATH
echo "security.protocol=$KAFKA_PROPERTIES_SECURITY_PROTOCOL" >> $CONNECTION_PROPERTIES_PATH

if [[ $KAFKA_PROPERTIES_SECURITY_PROTOCOL == "SSL" ]]; then
    echo "ssl.keystore.location=$KAFKA_PROPERTIES_SSL_KEYSTORE_LOCATION" >> $CONNECTION_PROPERTIES_PATH
    echo "ssl.keystore.password=$KAFKA_PROPERTIES_SSL_KEYSTORE_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
    echo "ssl.key.password=$KAFKA_PROPERTIES_SSL_KEY_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
    echo "ssl.truststore.location=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_LOCATION" >> $CONNECTION_PROPERTIES_PATH
    echo "ssl.truststore.password=$KAFKA_PROPERTIES_SSL_TRUSTSTORE_PASSWORD" >> $CONNECTION_PROPERTIES_PATH
    echo "ssl.endpoint.identification.algorithm=$KAFKA_PROPERTIES_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM" >> $CONNECTION_PROPERTIES_PATH
fi

cub kafka-ready -c $CONNECTION_PROPERTIES_PATH -b $KAFKA_BOOTSTRAP_SERVER 1 60 && \
kafka-topics --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_AUDIT_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $METADATA_CHANGE_EVENT_NAME && \
kafka-topics --create --if-not-exists --command-config $CONNECTION_PROPERTIES_PATH --zookeeper $KAFKA_ZOOKEEPER_CONNECT --partitions $PARTITIONS --replication-factor $REPLICATION_FACTOR --topic $FAILED_METADATA_CHANGE_EVENT_NAME && \
kafka-acls --authorizer-properties zookeeper.connect=$KAFKA_ZOOKEEPER_CONNECT --add --allow-principal "User:CN=$KAFKA_PRINCIPAL" --producer --consumer --group '*' --topic $METADATA_AUDIT_EVENT_NAME & \
kafka-acls --authorizer-properties zookeeper.connect=$KAFKA_ZOOKEEPER_CONNECT --add --allow-principal "User:CN=$KAFKA_PRINCIPAL" --producer --consumer --group '*' --topic $METADATA_CHANGE_EVENT_NAME & \
kafka-acls --authorizer-properties zookeeper.connect=$KAFKA_ZOOKEEPER_CONNECT --add --allow-principal "User:CN=$KAFKA_PRINCIPAL" --producer --consumer --group '*' --topic $FAILED_METADATA_CHANGE_EVENT_NAME