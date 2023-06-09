import React from 'react';
import styled from 'styled-components';
import { Button, Image, Typography } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../../../../constants';

const Container = styled(Button)`
    margin-right: 12px;
    margin-left: 12px;
    margin-bottom: 12px;
    padding: 24px;
    width: 30%;
    height: 152px;
    display: flex;
    justify-content: center;
    border-radius: 4px;
    align-items: start;
    flex-direction: column;
    border: 1px solid ${ANTD_GRAY[4]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
    && {
        text-align: start;
    }
    white-space: unset;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 12px;
`;

const Title = styled(Typography.Title)`
    && {
        padding: 0px;
        margin: 0px;
    }
    margin-right: 8px;
`;

const StyledClockCircleOutlined = styled(ClockCircleOutlined)`
    && {
        margin: 0px;
        padding: 0px;
        margin-right: 8px;
        font-size: 18px;
    }
`;

const Description = styled(Typography.Paragraph)`
    font-weight: normal;
`;

interface TypeOptionProps {
    name: string;
    description: string;
    imageSrc?: string | null;
    onClick: () => void;
}

/**
 * A specific Assertion Type option.
 */
export function AssertionTypeOption({ name, description, imageSrc, onClick }: TypeOptionProps) {
    return (
        <Container onClick={onClick}>
            <Header>
                <StyledClockCircleOutlined />
                <Title level={4}>{name}</Title>
            </Header>
            {imageSrc && <Image src={imageSrc} />}
            <Description type="secondary">{description}</Description>
        </Container>
    );
}
