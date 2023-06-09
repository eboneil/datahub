import React from 'react';
import styled from 'styled-components';
import { Button, Collapse, Radio, Typography } from 'antd';
import { AssertionBuilderStep, StepProps } from '../types';
import {
    AssertionEvaluationParametersType,
    CronSchedule,
    DatasetSlaAssertionParameters,
    FixedIntervalSchedule,
    SlaAssertionScheduleType,
} from '../../../../../../../../../types.generated';
import { FixedIntervalScheduleBuilder } from './sla/FixedIntervalSchedulerBuilder';
import { CronScheduleBuilder } from './sla/CronScheduleBuilder';
import { DatasetSlaSourceBuilder } from './sla/DatasetSlaSourceBuilder';

const TypeLabel = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

const Step = styled.div`
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
`;

const Form = styled.div``;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 20px;
`;

const Controls = styled.div`
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
`;

const SourceDescription = styled(Typography.Paragraph)`
    margin-top: 12px;
`;

/**
 * Step for defining the Dataset SLA assertion
 */
export const ConfigureDatasetSlaAssertionStep = ({ state, updateState, goTo, prev }: StepProps) => {
    const slaAssertion = state.assertion?.slaAssertion;
    const slaSchedule = slaAssertion?.schedule;
    const slaScheduleType = slaSchedule?.type;
    const slaScheduleCron = slaSchedule?.cron;
    const slaScheduleFixedInterval = slaSchedule?.fixedInterval;
    const datasetSlaParameters = state.parameters?.datasetSlaParameters;

    const updateScheduleType = (scheduleType: SlaAssertionScheduleType) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                slaAssertion: {
                    ...state?.assertion?.slaAssertion,
                    schedule: {
                        ...state?.assertion?.slaAssertion?.schedule,
                        type: scheduleType,
                    },
                },
            },
        });
    };

    const updateFixedIntervalSchedule = (fixedInterval: FixedIntervalSchedule) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                slaAssertion: {
                    ...state?.assertion?.slaAssertion,
                    schedule: {
                        ...state?.assertion?.slaAssertion?.schedule,
                        fixedInterval,
                    },
                },
            },
        });
    };

    const updateCronSchedule = (cron: CronSchedule) => {
        updateState({
            ...state,
            assertion: {
                ...state.assertion,
                slaAssertion: {
                    ...state.assertion?.slaAssertion,
                    schedule: {
                        ...state.assertion?.slaAssertion?.schedule,
                        cron,
                    },
                },
            },
        });
    };

    const updateDatasetSlaAssertionParameters = (parameters: DatasetSlaAssertionParameters) => {
        updateState({
            ...state,
            parameters: {
                type: AssertionEvaluationParametersType.DatasetSla,
                datasetSlaParameters: {
                    sourceType: parameters.sourceType,
                    auditLog: parameters.auditLog as any,
                    field: parameters.field as any,
                },
            },
        });
    };

    return (
        <Step>
            <Form>
                <Section>
                    <TypeLabel level={5}>SLA Type</TypeLabel>
                    <Radio.Group value={slaScheduleType} onChange={(e) => updateScheduleType(e.target.value)}>
                        <Radio.Button value={SlaAssertionScheduleType.FixedInterval}>Fixed Interval</Radio.Button>
                        <Radio.Button value={SlaAssertionScheduleType.Cron}>Schedule</Radio.Button>
                    </Radio.Group>
                    <SourceDescription type="secondary">
                        {slaScheduleType === SlaAssertionScheduleType.FixedInterval
                            ? 'Define an expected change interval to monitor for this dataset'
                            : 'Define an expected change schedule to monitor for this dataset'}
                    </SourceDescription>
                </Section>
                <Section>
                    {slaScheduleType === SlaAssertionScheduleType.FixedInterval ? (
                        <FixedIntervalScheduleBuilder
                            value={slaScheduleFixedInterval as FixedIntervalSchedule}
                            onChange={updateFixedIntervalSchedule}
                        />
                    ) : (
                        <CronScheduleBuilder
                            title="Expected Change Schedule"
                            value={slaScheduleCron as CronSchedule}
                            onChange={updateCronSchedule}
                            actionText="Changes by"
                            descriptionText="Assertion will fail if this dataset has not changed by the schedule timed, or between two consecutive schedule times"
                        />
                    )}
                </Section>
                <Section>
                    <Collapse>
                        <Collapse.Panel key="Advanced" header="Advanced">
                            <DatasetSlaSourceBuilder
                                entityUrn={state.entityUrn as string}
                                platformUrn={state.platformUrn as string}
                                value={datasetSlaParameters as DatasetSlaAssertionParameters}
                                onChange={updateDatasetSlaAssertionParameters}
                            />
                        </Collapse.Panel>
                    </Collapse>
                </Section>
            </Form>
            <Controls>
                <Button onClick={prev}>Back</Button>
                <Button type="primary" onClick={() => goTo(AssertionBuilderStep.CONFIGURE_SCHEDULE)}>
                    Next
                </Button>
            </Controls>
        </Step>
    );
};
