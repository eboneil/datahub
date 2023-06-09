import { Typography } from 'antd';
import React from 'react';
import cronstrue from 'cronstrue';
import {
    CronSchedule,
    FixedIntervalSchedule,
    SlaAssertionInfo,
    SlaAssertionScheduleType,
    SlaAssertionType,
} from '../../../../../../types.generated';

type Props = {
    assertionInfo: SlaAssertionInfo;
};

const createCronText = (cronSchedule: CronSchedule) => {
    const { cron, timezone } = cronSchedule;
    return `${cronstrue.toString(cron).toLocaleLowerCase()} (${timezone})`;
};

const createFixedIntervalText = (fixedIntervalSchedule: FixedIntervalSchedule) => {
    const { multiple, unit } = fixedIntervalSchedule;
    return `every ${multiple} ${unit.toLocaleLowerCase()}s`;
};

/**
 * A human-readable description of an SLA Assertion.
 */
export const SlaAssertionDescription = ({ assertionInfo }: Props) => {
    const scheduleType = assertionInfo.schedule?.type;
    const slaType = assertionInfo.type;

    return (
        <div>
            <Typography.Text>
                <b>SLA</b>:{' '}
                {slaType === SlaAssertionType.DatasetChange ? 'Dataset is updated ' : 'Data Task is run successfully '}
                {scheduleType === SlaAssertionScheduleType.Cron
                    ? createCronText(assertionInfo.schedule?.cron as any)
                    : createFixedIntervalText(assertionInfo.schedule?.fixedInterval as any)}
            </Typography.Text>
        </div>
    );
};
