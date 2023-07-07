import React from 'react';
import styled from 'styled-components';
import { Select, Typography } from 'antd';
import { SchemaFieldSpec } from '../../../../../../../../../../types.generated';

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const ColumnSelect = styled(Select)`
    width: 200px;
`;

type Props = {
    fields: SchemaFieldSpec[];
    value?: SchemaFieldSpec | null;
    onChange: (newField: SchemaFieldSpec) => void;
};

/**
 * Builder used to construct an Freshness based on a field / column value.
 */
export const FieldValueSourceBuilder = ({ fields, value, onChange }: Props) => {
    const updateSchemaFieldSpec = (newPath: any) => {
        const spec = fields.filter((field) => field.path === newPath)[0];
        onChange(spec);
    };

    return (
        <Form>
            <Section>
                <ColumnSelect placeholder="Select a column..." value={value?.path} onChange={updateSchemaFieldSpec}>
                    {fields.map((field) => (
                        <Select.Option value={field.path}>{field.path}</Select.Option>
                    ))}
                </ColumnSelect>
            </Section>
            <Section>
                <Typography.Text type="secondary">
                    Select the column representing the latest update time for a given row. This column must have type
                    TIMESTAMP, DATE, or DATETIME.
                </Typography.Text>
            </Section>
        </Form>
    );
};
