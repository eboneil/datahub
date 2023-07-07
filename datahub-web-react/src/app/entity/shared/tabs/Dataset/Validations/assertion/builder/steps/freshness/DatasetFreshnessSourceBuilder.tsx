import React from 'react';
import styled from 'styled-components';
import { Radio, Typography } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import {
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    SchemaFieldSpec,
} from '../../../../../../../../../../types.generated';
import { FieldValueSourceBuilder } from './FieldValueSourceBuilder';
import { getSourceTypesForPlatform, SOURCE_TYPE_TO_INFO } from '../../utils';
import { TIMESTAMP_FIELD_TYPES } from '../../constants';
import { useGetDatasetSchemaQuery } from '../../../../../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../../../../../constants';

const Form = styled.div``;

const SourceDescription = styled.div`
    margin-top: 12px;
    margin-bottom: 12px;
`;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)`
    color: ${ANTD_GRAY[6]};
    margin-right: 4px;
`;

type Props = {
    entityUrn: string;
    platformUrn: string;
    value?: DatasetFreshnessAssertionParameters | null;
    onChange: (newParams: DatasetFreshnessAssertionParameters) => void;
};

/**
 * Builder used to configure the Source Type of Dataset Freshness assertion evaluations.
 * This represents the signal that is used as input when determining whether an Freshness has been
 * missed or not.
 *
 * If demand exists, we can extend this support customizing the audit-log based Freshness assertions to include;:
 *
 *     - Operation types (monitor for inserts, updates, deletes, create tables specifically)
 *     - Row counts (min number of rows that change for a given operation)
 *     - User who performed the update
 *
 * For applicable sources
 */
export const DatasetFreshnessSourceBuilder = ({ entityUrn, platformUrn, value, onChange }: Props) => {
    const sourceType = value?.sourceType || DatasetFreshnessSourceType.AuditLog;
    const field = value?.field;

    const { data } = useGetDatasetSchemaQuery({
        variables: {
            urn: entityUrn,
        },
        fetchPolicy: 'cache-first',
    });

    /**
     * Extract the schema fields eligible for selection. These must be timestamp type fields.
     */
    const dateFields =
        data?.dataset?.schemaMetadata?.fields?.filter((f) => TIMESTAMP_FIELD_TYPES.has(f.type) && f.nativeDataType) ||
        [];
    const dateFieldSpecs = dateFields.map((f) => ({
        path: f.fieldPath,
        type: f.type,
        nativeType: f.nativeDataType as string,
    }));

    /**
     * Extract the source type options on a per-platform basis, as some data platforms
     * do not support all of them. In the future, we may want a better place to declare these.
     */
    const sourceTypes = getSourceTypesForPlatform(platformUrn).filter((st) => SOURCE_TYPE_TO_INFO.has(st));
    const selectedSourceTypeInfo = SOURCE_TYPE_TO_INFO.get(sourceType);

    const updateSourceType = (newSourceType: DatasetFreshnessSourceType) => {
        onChange({
            ...value,
            sourceType: newSourceType,
        });
    };

    const updateFieldSpec = (newSpec: SchemaFieldSpec) => {
        onChange({
            ...value,
            field: newSpec,
        } as any);
    };

    return (
        <Form>
            <Typography.Title level={5}>Change Source</Typography.Title>
            <Typography.Paragraph type="secondary">
                Select the mechanism used to determine whether a change has been made to this dataset.
            </Typography.Paragraph>
            <Radio.Group value={sourceType} onChange={(e) => updateSourceType(e.target.value)}>
                {sourceTypes.map((st) => (
                    <Radio.Button value={SOURCE_TYPE_TO_INFO.get(st).type}>
                        {SOURCE_TYPE_TO_INFO.get(st).name}
                    </Radio.Button>
                ))}
            </Radio.Group>
            <SourceDescription>
                <StyledInfoCircleOutlined />
                {selectedSourceTypeInfo.description}
            </SourceDescription>
            {sourceType === DatasetFreshnessSourceType.FieldValue && (
                <FieldValueSourceBuilder fields={dateFieldSpecs} value={field} onChange={updateFieldSpec} />
            )}
        </Form>
    );
};
