import React, { useState } from 'react';
import { Button, Menu, Dropdown, Typography } from 'antd';
import { LeftOutlined, RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';

const CustomPaginationContainer = styled.div`
    display: flex;
    flex-direction: row;
    height: 32px;
`;
const NavButton = styled(Button)`
    margin: 4px 6px;
    cursor: pointer;
`;
const DescriptionText = styled(Typography.Text)`
    line-height: 32px;
`;
const VersionText = styled(Typography.Text)`
    padding: 0 4px;
    line-height: 32px;
    cursor: pointer;
`;
const VersionRightText = styled(Typography.Text)`
    padding-left: 4px;
    line-height: 32px;
    cursor: pointer;
`;

type Props = {
    onChange: (version1: number, version2: number) => void;
    maxVersion: number;
};

export default function CustomPagination({ onChange, maxVersion }: Props) {
    const [version1, setVersion1] = useState(maxVersion || 1);
    const [version2, setVersion2] = useState(maxVersion ? maxVersion - 1 : 0);

    const onNextClick = () => {
        setVersion1((v) => v - 1);
        setVersion2(version1 - 2);
        onChange(version1 - 1, version1 - 2);
    };
    const onPrevClick = () => {
        setVersion1((v) => v + 1);
        setVersion2(version1);
        onChange(version1 + 1, version1);
    };
    const onVersion1Click = ({ key }) => {
        const newVersion1 = parseInt(key, 10);
        setVersion1(newVersion1);
        if (version2 >= newVersion1) {
            setVersion2(newVersion1 - 1);
            onChange(newVersion1, newVersion1 - 1);
            return;
        }
        onChange(newVersion1, version2);
    };
    const onVersion2Click = ({ key }) => {
        setVersion2(parseInt(key, 10));
        onChange(version1, parseInt(key, 10));
    };

    const menu1 = (
        <Menu onClick={onVersion1Click} selectedKeys={[`${version1}`]}>
            {[...Array(maxVersion)].map((_, i) => (
                // eslint-disable-next-line react/no-array-index-key
                <Menu.Item key={i + 1}>
                    <Typography.Text>{`version ${i + 1}`}</Typography.Text>
                </Menu.Item>
            ))}
        </Menu>
    );

    const menu2 = (
        <Menu onClick={onVersion2Click} selectedKeys={[`${version2}`]}>
            {[...Array(version1)].map((_, i) => (
                // eslint-disable-next-line react/no-array-index-key
                <Menu.Item key={i}>
                    <Typography.Text>{`version ${i}`}</Typography.Text>
                </Menu.Item>
            ))}
        </Menu>
    );

    return (
        <CustomPaginationContainer>
            <NavButton
                size="small"
                type="text"
                icon={<LeftOutlined />}
                onClick={onPrevClick}
                disabled={version1 >= maxVersion}
            />
            <DescriptionText>Comparing</DescriptionText>
            <Dropdown overlay={menu1} trigger={['click']}>
                <VersionText strong type="success">{`version ${version1}`}</VersionText>
            </Dropdown>
            <DescriptionText>to</DescriptionText>
            <Dropdown overlay={menu2} trigger={['click']}>
                <VersionRightText strong type="success">{`version ${version2}`}</VersionRightText>
            </Dropdown>
            <NavButton
                size="small"
                type="text"
                icon={<RightOutlined />}
                onClick={onNextClick}
                disabled={version1 <= 1}
            />
        </CustomPaginationContainer>
    );
}
