#!/bin/bash

set -e

: "${DATAHUB_ANALYTICS_ENABLED:=true}"
: "${USE_AWS_ELASTICSEARCH:=false}"
: "${ELASTICSEARCH_INSECURE:=false}"

# protocol: http or https?
if [[ $ELASTICSEARCH_USE_SSL == true ]]; then
    ELASTICSEARCH_PROTOCOL=https
else
    ELASTICSEARCH_PROTOCOL=http
fi
echo -e "going to use protocol: $ELASTICSEARCH_PROTOCOL"

# Elasticsearch URL to be suffixed with a resource address
ELASTICSEARCH_URL="$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT"

if [[ -z $ELASTICSEARCH_MASTER_USERNAME ]]; then
  echo -e "Variable ELASTICSEARCH_MASTER_USERNAME is not set. Going to use value of ELASTICSEARCH_USERNAME"
  ELASTICSEARCH_MASTER_USERNAME=$ELASTICSEARCH_USERNAME
fi
if [[ -z $ELASTICSEARCH_MASTER_PASSWORD ]]; then
  echo -e "Variable ELASTICSEARCH_MASTER_PASSWORD is not set. Going to use value of ELASTICSEARCH_PASSWORD"
  ELASTICSEARCH_MASTER_PASSWORD=$ELASTICSEARCH_PASSWORD
fi

if [[ -n $ELASTICSEARCH_MASTER_USERNAME ]] && [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  AUTH_TOKEN=$(echo -ne "$ELASTICSEARCH_MASTER_USERNAME:$ELASTICSEARCH_MASTER_PASSWORD" | base64 --wrap 0)
  ELASTICSEARCH_AUTH_HEADER="Authorization:Basic $AUTH_TOKEN"
fi

# Add default header if needed
if [[ -z $ELASTICSEARCH_AUTH_HEADER ]]; then
  echo -e "Going to use default elastic headers"
  ELASTICSEARCH_AUTH_HEADER="Accept: */*"
fi

# will be using this for all curl communication with Elasticsearch:
CURL_ARGS=(
  --silent
  --header "$ELASTICSEARCH_AUTH_HEADER"
)
# ... also optionally use --insecure
if [[ $ELASTICSEARCH_INSECURE == true ]]; then
  CURL_ARGS+=(--insecure)
fi

# index prefix used throughout the script
if [[ -z "$INDEX_PREFIX" ]]; then
  PREFIX=''
  echo -e "not using any prefix"
else
  PREFIX="${INDEX_PREFIX}_"
  echo -e "going to use prefix: '$PREFIX'"
fi

# path where index definitions are stored
INDEX_DEFINITIONS_ROOT=/index/usage-event


# check Elasticsearch for given index/resource (first argument)
# if it doesn't exist (http code 404), use the given file (second argument) to create it
function create_if_not_exists() {
  RESOURCE_ADDRESS="$1"
  RESOURCE_DEFINITION_NAME="$2"

  # query ES to see if the resource already exists
  RESOURCE_STATUS=$(curl "${CURL_ARGS[@]}" -o request_response.txt -w "%{http_code}\n" "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS")
  echo -e "\n>>> GET $RESOURCE_ADDRESS response code is $RESOURCE_STATUS"

  if [ "$RESOURCE_STATUS" -eq 200 ]; then
    # resource already exists -> nothing to do
    echo -e ">>> $RESOURCE_ADDRESS already exists ✓"

  elif [ "$RESOURCE_STATUS" -eq 404 ]; then
    # resource doesn't exist -> need to create it
    echo -e ">>> creating $RESOURCE_ADDRESS because it doesn't exist ..."
    # use the file at given path as definition, but first replace all occurences of `PREFIX`
    # placeholder within the file with the actual prefix value
    TMP_SOURCE_PATH="/tmp/$RESOURCE_DEFINITION_NAME"
    sed -e "s/PREFIX/$PREFIX/g" "$INDEX_DEFINITIONS_ROOT/$RESOURCE_DEFINITION_NAME" | tee -a "$TMP_SOURCE_PATH"
    curl "${CURL_ARGS[@]}" -XPUT "$ELASTICSEARCH_URL/$RESOURCE_ADDRESS" -H 'Content-Type: application/json' --data "@$TMP_SOURCE_PATH"

  elif [ "$RESOURCE_STATUS" -eq 403 ]; then
    # probably authorization fail
    echo -e ">>> forbidden access to $RESOURCE_ADDRESS ! -> exiting"
    cat request_response.txt
    rm request_response.txt
    exit 1

  else
    # when `USE_AWS_ELASTICSEARCH` was forgotten to be set to `true` when running against AWS ES OSS,
    # this script will use wrong paths (e.g. `_ilm/policy/` instead of AWS-compatible `_opendistro/_ism/policies/`)
    # and the ES endpoint will return `401 Unauthorized` or `405 Method Not Allowed`
    # let's use this as chance to point that wrong config might be used!
    if [ "$RESOURCE_STATUS" -eq 401 ] || [ "$RESOURCE_STATUS" -eq 405 ]; then
      if [[ "$USE_AWS_ELASTICSEARCH" == false ]] && [[ "$ELASTICSEARCH_URL" == *"amazonaws"* ]]; then
        echo "... looks like AWS OpenSearch is used; please set USE_AWS_ELASTICSEARCH env value to true"
      fi
    fi

    echo -e ">>> failed to GET $RESOURCE_ADDRESS ! -> exiting"
    cat request_response.txt
    rm request_response.txt
    exit 1
  fi
}

# create indices for ES (non-AWS)
function create_datahub_usage_event_datastream() {
  # non-AWS env requires creation of two resources for Datahub usage events:
  #   1. ILM policy
  create_if_not_exists "_ilm/policy/${PREFIX}datahub_usage_event_policy" policy.json
  #   2. index template
  create_if_not_exists "_index_template/${PREFIX}datahub_usage_event_index_template" index_template.json
}

# create indices for ES OSS (AWS)
function create_datahub_usage_event_aws_elasticsearch() {
  # AWS env requires creation of three resources for Datahub usage events:
  #   1. ISM policy
  create_if_not_exists "_opendistro/_ism/policies/${PREFIX}datahub_usage_event_policy" aws_es_ism_policy.json
  echo -e "\nISM policy created"
  #   2. index template
  create_if_not_exists "_template/${PREFIX}datahub_usage_event_index_template" aws_es_index_template.json
  echo -e "\nIndex template created"

  #   3. event index datahub_usage_event-000001
  #     (note that AWS *rollover* indices need to use `^.*-\d+$` naming pattern)
  #     -> https://aws.amazon.com/premiumsupport/knowledge-center/opensearch-failed-rollover-index/
  INDEX_SUFFIX="000001"
  #     ... but first check whether `datahub_usage_event` wasn't already autocreated by GMS before `datahub_usage_event-000001`
  #     (as is common case when this script was initially run without properly setting `USE_AWS_ELASTICSEARCH` to `true`)
  #     -> https://github.com/datahub-project/datahub/issues/5376
  USAGE_EVENT_STATUS=$(curl "${CURL_ARGS[@]}" -o request_response.txt -w "%{http_code}\n" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
  if [ "$USAGE_EVENT_STATUS" -eq 200 ]; then
    USAGE_EVENT_DEFINITION=$(curl "${CURL_ARGS[@]}" "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event")
    # the definition is expected to contain "datahub_usage_event-000001" string
    if [[ "$USAGE_EVENT_DEFINITION" != *"datahub_usage_event-$INDEX_SUFFIX"* ]]; then
      # ... if it doesn't, we need to drop it
      echo -e "\n>>> deleting invalid datahub_usage_event ..."
      curl "${CURL_ARGS[@]}" -XDELETE "$ELASTICSEARCH_URL/${PREFIX}datahub_usage_event"
      # ... and then recreate it below
    fi
  else
    echo -e "Usage event status: $USAGE_EVENT_STATUS"
    echo request_response.txt
    rm request_response.txt
  fi

  #   ... now we are safe to create the index
  create_if_not_exists "${PREFIX}datahub_usage_event-$INDEX_SUFFIX" aws_es_index.json
}

function create_access_policy_data_es_cloud {
  cat << EOF
{
    "cluster":[ "monitor" ],
    "indices":[
       {
          "names":["${INDEX_PREFIX}_*"],
          "privileges":["all"]
       }
    ]
 }
EOF
}

function create_user_data_es_cloud {
  cat <<EOF
{
    "password": "${ELASTICSEARCH_PASSWORD}",
 	  "roles":["${INDEX_PREFIX}_access"]
 }
EOF
}

function create_aws_access_policy_role {
  cat << EOF
{
     "cluster_permissions": [
         "indices:*",
         "cluster:monitor/tasks/lists"
     ],
     "index_permissions": [
         {
             "index_patterns": [
                 "${INDEX_PREFIX}_*"
             ],
             "allowed_actions": [
                 "indices_all"
             ]
         }
     ]
 }
EOF
}

function create_aws_data_user {
  cat << EOF
{
     "password": "${ELASTICSEARCH_PASSWORD}",
     "opendistro_security_roles": [
         "${ROLE}"
     ]
 }
EOF
}

function create_user_es_cloud {
  # Tested with Elastic 7.17
  ROLE="${INDEX_PREFIX}_access"

  create_access_policy_data_es_cloud > $INDEX_DEFINITIONS_ROOT/access_policy_data_es_cloud.json
  create_if_not_exists "_security/role/${ROLE}" access_policy_data_es_cloud.json
  echo -e "\nAccess policy created"

  create_user_data_es_cloud > $INDEX_DEFINITIONS_ROOT/user_data_es_cloud.json
  create_if_not_exists "_security/user/${ELASTICSEARCH_USERNAME}" user_data_es_cloud.json
  echo -e "\nData User created"
}

function create_aws_user {
  ROLE="${INDEX_PREFIX}_access"

  create_aws_access_policy_role > $INDEX_DEFINITIONS_ROOT/aws_role.json
  create_if_not_exists "_opendistro/_security/api/roles/${ROLE}" aws_role.json
  echo -e "\nAWS Access policy created"

  create_aws_data_user > $INDEX_DEFINITIONS_ROOT/aws_user.json
  create_if_not_exists "_opendistro/_security/api/internalusers/${ELASTICSEARCH_USERNAME}" aws_user.json
  echo -e "\nAWS Data User created"
}

if [[ $CREATE_USER == true ]]; then
  echo -e "\nCreating user"
  if [[ $USE_AWS_ELASTICSEARCH == true ]]; then
    create_aws_user || exit 1
  else
    create_user_es_cloud || exit 1
  fi
fi

if [[ $DATAHUB_ANALYTICS_ENABLED == true ]]; then
  echo -e "\nCreating indices for analytics"
  if [[ $USE_AWS_ELASTICSEARCH == false ]]; then
    create_datahub_usage_event_datastream || exit 1
  else
    create_datahub_usage_event_aws_elasticsearch || exit 1
  fi
else
  echo -e "\nCreating usage index"
  DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE=$(curl -o /dev/null -s -w "%{http_code}" --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/cat/indices/${PREFIX}datahub_usage_event")
  if [ "$DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE" -eq 404 ]
  then
    echo -e "\ncreating ${PREFIX}datahub_usage_event"
    curl -XPUT --header "$ELASTICSEARCH_AUTH_HEADER" "${ELASTICSEARCH_INSECURE}$ELASTICSEARCH_PROTOCOL://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/${PREFIX}datahub_usage_event"
  elif [ "$DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE" -eq 200 ]; then
    echo -e "\n${PREFIX}datahub_usage_event exists"
  elif [ "$DATAHUB_USAGE_EVENT_INDEX_RESPONSE_CODE" -eq 403 ]; then
    echo -e "Forbidden so exiting"
  fi
fi