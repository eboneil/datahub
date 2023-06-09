import {
    DatasetSlaSourceType,
    EntityType,
    SlaAssertionScheduleType,
    SlaAssertionType,
} from '../../../../../../../../types.generated';
import { BIGQUERY_URN, REDSHIFT_URN, SNOWFLAKE_URN } from '../../../../../../../ingest/source/builder/constants';
import { AssertionMonitorBuilderState } from './types';
import { ASSERTION_TYPES } from './constants';

export const builderStateToUpdateSlaAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            type: builderState.assertion?.slaAssertion?.type as SlaAssertionType,
            schedule: {
                type: builderState.assertion?.slaAssertion?.schedule?.type as SlaAssertionScheduleType,
                cron: builderState.assertion?.slaAssertion?.schedule?.cron,
                fixedInterval: builderState.assertion?.slaAssertion?.schedule?.fixedInterval,
            },
            actions: builderState.assertion?.actions
                ? {
                      onSuccess: builderState.assertion?.actions?.onSuccess || [],
                      onFailure: builderState.assertion?.actions?.onFailure || [],
                  }
                : undefined,
        },
    };
};

export const builderStateToCreateAssertionMonitorVariables = (
    assertionUrn: string,
    builderState: AssertionMonitorBuilderState,
) => {
    return {
        input: {
            entityUrn: builderState?.entityUrn,
            assertionUrn,
            schedule: builderState.schedule,
            parameters: builderState.parameters,
        },
    };
};

export const builderStateToCreateSlaAssertionVariables = (builderState: AssertionMonitorBuilderState) => {
    return {
        input: {
            entityUrn: builderState.entityUrn as string,
            ...builderStateToUpdateSlaAssertionVariables(builderState).input,
        },
    };
};

export const getAssertionTypesForEntityType = (entityType: EntityType) => {
    return ASSERTION_TYPES.filter((type) => type.entityTypes.includes(entityType));
};

export const SOURCE_TYPES = [
    {
        type: DatasetSlaSourceType.AuditLog,
        name: 'Audit Log',
        description: 'Use operations logged in the platform audit log to determine whether the dataset has changed',
    },
    {
        type: DatasetSlaSourceType.InformationSchema,
        name: 'Information Schema',
        description:
            'Use the information schema or system metadata tables to determine whether the dataset has changed',
    },
    {
        type: DatasetSlaSourceType.FieldValue,
        name: 'Date Column',
        description:
            'Check the maximum value of a timestamp or date column to determine whether the dataset has changed',
    },
];

export const SOURCE_TYPE_TO_INFO = new Map();
SOURCE_TYPES.forEach((type) => {
    SOURCE_TYPE_TO_INFO.set(type.type, type);
});

export const getSourceTypesForPlatform = (platformUrn: string) => {
    switch (platformUrn) {
        case SNOWFLAKE_URN:
            return [
                DatasetSlaSourceType.AuditLog,
                DatasetSlaSourceType.InformationSchema,
                DatasetSlaSourceType.FieldValue,
            ];
        case BIGQUERY_URN:
            return [
                DatasetSlaSourceType.AuditLog,
                DatasetSlaSourceType.InformationSchema,
                DatasetSlaSourceType.FieldValue,
            ];
        case REDSHIFT_URN:
            return [DatasetSlaSourceType.AuditLog, DatasetSlaSourceType.FieldValue];
        default:
            return []; // No types supported.
    }
};

/**
 * Returns true if the entity is eligible for online assertion monitoring.
 * Currently limited to Snowflake, Redshift, and BigQuery.
 */
const ASSERTION_SUPPORTED_PLATFORM_URNS = [SNOWFLAKE_URN, REDSHIFT_URN, BIGQUERY_URN];
export const isEntityEligibleForAssertionMonitoring = (platformUrn) => {
    if (!platformUrn) {
        return false;
    }
    return ASSERTION_SUPPORTED_PLATFORM_URNS.includes(platformUrn);
};

export const adjustCronText = (text: string) => {
    return text.replace('at', '');
};
