import React from 'react';
import styled from 'styled-components/macro';
import { Popover, Typography } from 'antd';
import { ClockCircleOutlined, ConsoleSqlOutlined, TableOutlined, TeamOutlined, HddOutlined } from '@ant-design/icons';
import { formatNumberWithoutAbbreviation } from '../../../shared/formatNumber';
import { ANTD_GRAY } from '../../shared/constants';
import { toLocalDateTimeString, toRelativeTimeString } from '../../../shared/time/timeUtils';
import { StatsSummary } from '../../shared/components/styled/StatsSummary';
import { FormattedBytesStat } from './FormattedBytesStat';
import { PercentileLabel } from '../../shared/stats/PercentileLabel';

const StatText = styled.span<{ color: string }>`
    color: ${(props) => props.color};
`;

const PopoverContent = styled.div`
    max-width: 300px;
`;

type Props = {
    rowCount?: number | null;
    columnCount?: number | null;
    sizeInBytes?: number | null;
    queryCountLast30Days?: number | null;
    queryCountPercentileLast30Days?: number | null;
    uniqueUserCountLast30Days?: number | null;
    uniqueUserPercentileLast30Days?: number | null;
    lastUpdatedMs?: number | null;
    color?: string;
};

export const DatasetStatsSummary = ({
    rowCount,
    columnCount,
    sizeInBytes,
    queryCountLast30Days,
    queryCountPercentileLast30Days,
    uniqueUserCountLast30Days,
    uniqueUserPercentileLast30Days,
    lastUpdatedMs,
    color,
}: Props) => {
    const displayedColor = color !== undefined ? color : ANTD_GRAY[7];

    const statsViews = [
        !!rowCount && (
            <StatText color={displayedColor}>
                <TableOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(rowCount)}</b> rows
                {!!columnCount && (
                    <>
                        , <b>{formatNumberWithoutAbbreviation(columnCount)}</b> columns
                    </>
                )}
            </StatText>
        ),
        !!sizeInBytes && (
            <StatText color={displayedColor}>
                <HddOutlined style={{ marginRight: 8, color: displayedColor }} />
                <FormattedBytesStat bytes={sizeInBytes} />
            </StatText>
        ),
        !!queryCountLast30Days && (
            <StatText color={displayedColor}>
                <ConsoleSqlOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(queryCountLast30Days)}</b> queries last month
                {!!queryCountPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        -{' '}
                        <PercentileLabel
                            percentile={queryCountPercentileLast30Days}
                            description={`This dataset has been queried more often than ${queryCountPercentileLast30Days}% of similar datasets in the past 30 days.`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        ),
        !!uniqueUserCountLast30Days && (
            <StatText color={displayedColor}>
                <TeamOutlined style={{ marginRight: 8, color: displayedColor }} />
                <b>{formatNumberWithoutAbbreviation(uniqueUserCountLast30Days)}</b> unique users
                {!!uniqueUserPercentileLast30Days && (
                    <Typography.Text type="secondary">
                        -{' '}
                        <PercentileLabel
                            percentile={uniqueUserPercentileLast30Days}
                            description={`This dataset has had more unique users than ${uniqueUserPercentileLast30Days}% of similar datasets in the past 30 days.`}
                        />
                    </Typography.Text>
                )}
            </StatText>
        ),
        !!lastUpdatedMs && (
            <Popover
                content={
                    <PopoverContent>
                        Data was last updated in the source platform on{' '}
                        <strong>{toLocalDateTimeString(lastUpdatedMs)}</strong>
                    </PopoverContent>
                }
            >
                <StatText color={displayedColor}>
                    <ClockCircleOutlined style={{ marginRight: 8, color: ANTD_GRAY[7] }} />
                    Updated {toRelativeTimeString(lastUpdatedMs)}
                </StatText>
            </Popover>
        ),
    ].filter((stat) => stat);

    return <>{statsViews.length > 0 && <StatsSummary stats={statsViews} />}</>;
};
