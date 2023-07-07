package com.linkedin.metadata;

public class AcrylConstants {

  public static final String ACTION_REQUEST_TYPE_TERM_PROPOSAL = "TERM_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_TAG_PROPOSAL = "TAG_ASSOCIATION";
  public static final String ACTION_REQUEST_TYPE_CREATE_GLOSSARY_NODE_PROPOSAL = "CREATE_GLOSSARY_NODE";
  public static final String ACTION_REQUEST_TYPE_CREATE_GLOSSARY_TERM_PROPOSAL = "CREATE_GLOSSARY_TERM";
  public static final String ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL = "UPDATE_DESCRIPTION";
  public static final String ACTION_REQUEST_STATUS_PENDING = "PENDING";
  public static final String ACTION_REQUEST_STATUS_COMPLETE = "COMPLETED";
  public static final String ACTION_REQUEST_RESULT_ACCEPTED = "ACCEPTED";
  public static final String ACTION_REQUEST_RESULT_REJECTED = "REJECTED";

  // For entity change events
  public static final String ACTION_REQUEST_STATUS_KEY = "actionRequestStatus";
  public static final String ACTION_REQUEST_TYPE_KEY = "actionRequestType";
  public static final String ACTION_REQUEST_RESULT_KEY = "actionRequestResult";
  public static final String RESOURCE_TYPE_KEY = "resourceType";
  public static final String RESOURCE_URN_KEY = "resourceUrn";
  public static final String SUB_RESOURCE_TYPE_KEY = "subResourceType";
  public static final String SUB_RESOURCE_KEY = "subResource";
  public static final String GLOSSARY_TERM_URN_KEY = "glossaryTermUrn";
  public static final String TAG_URN_KEY = "tagUrn";
  public static final String GLOSSARY_ENTITY_NAME_KEY = "glossaryEntityName";
  public static final String PARENT_NODE_URN_KEY = "parentNodeUrn";
  public static final String DESCRIPTION_KEY = "description";

  // For tests
  public static final String PASSING_TESTS_FIELD = "passingTests";
  public static final String FAILING_TESTS_FIELD = "failingTests";
  public static final String TESTS_CREATED_TIME_INDEX_FIELD_NAME = "createdTime";
  public static final String TESTS_LAST_UPDATED_TIME_INDEX_FIELD_NAME = "lastUpdatedTime";

  // For system monitors
  public static final String FRESHNESS_SYSTEM_MONITOR_ID = "__system_freshness";
  public static final String ACRYL_LOGO_FILE_PATH = "/integrations/static/acryl-slack-icon.png";

  private AcrylConstants() {
  }
}

