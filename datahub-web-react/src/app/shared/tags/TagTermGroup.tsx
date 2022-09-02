import { Modal, Tag, Typography, Button, message, Tooltip } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { BookOutlined, ClockCircleOutlined, PlusOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    Domain,
    ActionRequest,
    EntityType,
    GlobalTags,
    GlossaryTermAssociation,
    GlossaryTerms,
    SubResourceType,
    TagAssociation,
} from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { EMPTY_MESSAGES, ANTD_GRAY } from '../../entity/shared/constants';
import { useRemoveTagMutation, useRemoveTermMutation } from '../../../graphql/mutations.generated';
import { DomainLink } from './DomainLink';
import { TagProfileDrawer } from './TagProfileDrawer';
import { useAcceptProposalMutation, useRejectProposalMutation } from '../../../graphql/actionRequest.generated';
import ProposalModal from './ProposalModal';
import EditTagTermsModal from './AddTagsTermsModal';
import { HoverEntityTooltip } from '../../recommendations/renderer/component/HoverEntityTooltip';

const PropagateThunderbolt = styled(ThunderboltOutlined)`
    color: rgba(0, 143, 100, 0.95);
    margin-right: -4px;
    font-weight: bold;
`;

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    editableGlossaryTerms?: GlossaryTerms | null;
    uneditableGlossaryTerms?: GlossaryTerms | null;
    domain?: Domain | undefined | null;
    canRemove?: boolean;
    canAddTag?: boolean;
    canAddTerm?: boolean;
    showEmptyMessage?: boolean;
    buttonProps?: Record<string, unknown>;
    onOpenModal?: () => void;
    maxShow?: number;
    entityUrn?: string;
    entityType?: EntityType;
    entitySubresource?: string;
    refetch?: () => Promise<any>;

    proposedGlossaryTerms?: ActionRequest[];
    proposedTags?: ActionRequest[];
};

const TermLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const TagLink = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const NoElementButton = styled(Button)`
    :not(:last-child) {
        margin-right: 8px;
    }
`;

const TagText = styled.span`
    color: ${ANTD_GRAY[7]};
`;

const ProposedTerm = styled(Tag)`
    opacity: 0.7;
    border-style: dashed;
`;

const PROPAGATOR_URN = 'urn:li:corpuser:__datahub_propagator';

export default function TagTermGroup({
    uneditableTags,
    editableTags,
    canRemove,
    canAddTag,
    canAddTerm,
    showEmptyMessage,
    buttonProps,
    onOpenModal,
    maxShow,
    uneditableGlossaryTerms,
    editableGlossaryTerms,
    proposedGlossaryTerms,
    proposedTags,
    domain,
    entityUrn,
    entityType,
    entitySubresource,
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);

    const [acceptProposalMutation] = useAcceptProposalMutation();
    const [rejectProposalMutation] = useRejectProposalMutation();
    const [showProposalDecisionModal, setShowProposalDecisionModal] = useState(false);

    const tagsEmpty =
        !editableTags?.tags?.length &&
        !uneditableTags?.tags?.length &&
        !editableGlossaryTerms?.terms?.length &&
        !uneditableGlossaryTerms?.terms?.length &&
        !proposedTags?.length &&
        !proposedGlossaryTerms?.length;
    const [removeTagMutation] = useRemoveTagMutation();
    const [removeTermMutation] = useRemoveTermMutation();
    const [tagProfileDrawerVisible, setTagProfileDrawerVisible] = useState(false);
    const [addTagUrn, setAddTagUrn] = useState('');

    const removeTag = (tagAssociationToRemove: TagAssociation) => {
        const tagToRemove = tagAssociationToRemove.tag;
        onOpenModal?.();
        Modal.confirm({
            title: `Do you want to remove ${tagToRemove?.name} tag?`,
            content: `Are you sure you want to remove the ${tagToRemove?.name} tag?`,
            onOk() {
                if (tagAssociationToRemove.associatedUrn || entityUrn) {
                    removeTagMutation({
                        variables: {
                            input: {
                                tagUrn: tagToRemove.urn,
                                resourceUrn: tagAssociationToRemove.associatedUrn || entityUrn || '',
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: 'Removed Tag!', duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: `Failed to remove tag: \n ${e.message || ''}`, duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const removeTerm = (termToRemove: GlossaryTermAssociation) => {
        onOpenModal?.();
        const termName = termToRemove && entityRegistry.getDisplayName(termToRemove.term.type, termToRemove.term);
        Modal.confirm({
            title: `Do you want to remove ${termName} term?`,
            content: `Are you sure you want to remove the ${termName} term?`,
            onOk() {
                if (termToRemove.associatedUrn || entityUrn) {
                    removeTermMutation({
                        variables: {
                            input: {
                                termUrn: termToRemove.term.urn,
                                resourceUrn: termToRemove.associatedUrn || entityUrn || '',
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: 'Removed Term!', duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: `Failed to remove term: \n ${e.message || ''}`, duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    let renderedTags = 0;

    const showTagProfileDrawer = (urn: string) => {
        setTagProfileDrawerVisible(true);
        setAddTagUrn(urn);
    };

    const closeTagProfileDrawer = () => {
        setTagProfileDrawerVisible(false);
    };

    const onCloseProposalDecisionModal = (e) => {
        e.stopPropagation();
        setShowProposalDecisionModal(false);
        setTimeout(() => refetch?.(), 2000);
    };

    const onProposalAcceptance = (actionRequest: ActionRequest) => {
        acceptProposalMutation({ variables: { urn: actionRequest.urn } })
            .then(() => {
                message.success('Successfully accepted the proposal!');
            })
            .then(refetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. :(');
            });
    };

    const onProposalRejection = (actionRequest: ActionRequest) => {
        rejectProposalMutation({ variables: { urn: actionRequest.urn } })
            .then(() => {
                message.info('Proposal declined.');
            })
            .then(refetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposal. :(');
            });
    };

    const onActionRequestUpdate = () => {
        refetch?.();
    };

    return (
        <>
            {domain && (
                <DomainLink domain={domain} name={entityRegistry.getDisplayName(EntityType.Domain, domain) || ''} />
            )}
            {uneditableGlossaryTerms?.terms?.map((term) => {
                renderedTags += 1;
                if (maxShow && renderedTags === maxShow + 1)
                    return (
                        <TagText>
                            {uneditableGlossaryTerms?.terms
                                ? `+${uneditableGlossaryTerms?.terms?.length - maxShow}`
                                : null}
                        </TagText>
                    );
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <HoverEntityTooltip entity={term.term}>
                        <TermLink
                            to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)}
                            key={term.term.urn}
                        >
                            <Tag closable={false} style={{ cursor: 'pointer' }}>
                                <BookOutlined style={{ marginRight: '3%' }} />
                                {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                            </Tag>
                        </TermLink>
                    </HoverEntityTooltip>
                );
            })}
            {editableGlossaryTerms?.terms?.map((term) => (
                <HoverEntityTooltip entity={term.term}>
                    <TermLink
                        to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)}
                        key={term.term.urn}
                    >
                        <Tag
                            style={{ cursor: 'pointer' }}
                            closable={canRemove}
                            onClose={(e) => {
                                e.preventDefault();
                                removeTerm(term);
                            }}
                        >
                            <BookOutlined style={{ marginRight: '3%' }} />
                            {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                            {term.actor?.urn === PROPAGATOR_URN && <PropagateThunderbolt />}
                        </Tag>
                    </TermLink>
                </HoverEntityTooltip>
            ))}
            {proposedGlossaryTerms?.map((actionRequest) => (
                <Tooltip overlay="Pending approval from owners">
                    <ProposedTerm
                        closable={false}
                        data-testid={`proposed-term-${actionRequest.params?.glossaryTermProposal?.glossaryTerm?.name}`}
                        onClick={() => {
                            setShowProposalDecisionModal(true);
                        }}
                    >
                        <BookOutlined style={{ marginRight: '3%' }} />
                        {entityRegistry.getDisplayName(
                            EntityType.GlossaryTerm,
                            actionRequest.params?.glossaryTermProposal?.glossaryTerm,
                        )}
                        <ProposalModal
                            actionRequest={actionRequest}
                            showProposalDecisionModal={showProposalDecisionModal}
                            onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                            onProposalAcceptance={onProposalAcceptance}
                            onProposalRejection={onProposalRejection}
                            onActionRequestUpdate={onActionRequestUpdate}
                            elementName={entityRegistry.getDisplayName(
                                EntityType.GlossaryTerm,
                                actionRequest.params?.glossaryTermProposal?.glossaryTerm,
                            )}
                        />
                        <ClockCircleOutlined style={{ color: 'orange', marginLeft: '3%' }} />
                    </ProposedTerm>
                </Tooltip>
            ))}
            {/* uneditable tags are provided by ingestion pipelines exclusively */}
            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags === maxShow + 1)
                    return (
                        <TagText>{uneditableTags?.tags ? `+${uneditableTags?.tags?.length - maxShow}` : null}</TagText>
                    );
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <HoverEntityTooltip entity={tag?.tag}>
                        <TagLink key={tag?.tag?.urn}>
                            <StyledTag
                                style={{ cursor: 'pointer' }}
                                onClick={() => showTagProfileDrawer(tag?.tag?.urn)}
                                $colorHash={tag?.tag?.urn}
                                $color={tag?.tag?.properties?.colorHex}
                                closable={false}
                            >
                                {entityRegistry.getDisplayName(EntityType.Tag, tag.tag)}
                            </StyledTag>
                        </TagLink>
                    </HoverEntityTooltip>
                );
            })}
            {/* editable tags may be provided by ingestion pipelines or the UI */}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <HoverEntityTooltip entity={tag?.tag}>
                        <TagLink>
                            <StyledTag
                                style={{ cursor: 'pointer' }}
                                onClick={() => showTagProfileDrawer(tag?.tag?.urn)}
                                $colorHash={tag?.tag?.urn}
                                $color={tag?.tag?.properties?.colorHex}
                                closable={canRemove}
                                onClose={(e) => {
                                    e.preventDefault();
                                    removeTag(tag);
                                }}
                            >
                                {tag?.tag?.name}
                            </StyledTag>
                        </TagLink>
                    </HoverEntityTooltip>
                );
            })}
            {proposedTags?.map((actionRequest) => (
                <Tooltip overlay="Pending approval from owners">
                    <StyledTag
                        data-testid={`proposed-tag-${actionRequest?.params?.tagProposal?.tag?.name}`}
                        $colorHash={actionRequest?.params?.tagProposal?.tag?.urn}
                        $color={actionRequest?.params?.tagProposal?.tag?.properties?.colorHex}
                        onClick={() => {
                            setShowProposalDecisionModal(true);
                        }}
                    >
                        {actionRequest?.params?.tagProposal?.tag?.name}
                        <ProposalModal
                            actionRequest={actionRequest}
                            showProposalDecisionModal={showProposalDecisionModal}
                            onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                            onProposalAcceptance={onProposalAcceptance}
                            onProposalRejection={onProposalRejection}
                            onActionRequestUpdate={onActionRequestUpdate}
                            elementName={actionRequest?.params?.tagProposal?.tag?.name}
                        />
                        <ClockCircleOutlined style={{ color: 'orange', marginLeft: '3%' }} />
                    </StyledTag>
                </Tooltip>
            ))}
            {tagProfileDrawerVisible && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
            {showEmptyMessage && canAddTag && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {EMPTY_MESSAGES.tags.title}. {EMPTY_MESSAGES.tags.description}
                </Typography.Paragraph>
            )}
            {showEmptyMessage && canAddTerm && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {EMPTY_MESSAGES.terms.title}. {EMPTY_MESSAGES.terms.description}
                </Typography.Paragraph>
            )}
            {canAddTag && (
                <NoElementButton
                    type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                    onClick={() => {
                        setAddModalType(EntityType.Tag);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <PlusOutlined />
                    <span>Add Tags</span>
                </NoElementButton>
            )}
            {canAddTerm && (
                <NoElementButton
                    type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                    onClick={() => {
                        setAddModalType(EntityType.GlossaryTerm);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <PlusOutlined />
                    <span>Add Terms</span>
                </NoElementButton>
            )}
            {showAddModal && !!entityUrn && !!entityType && (
                <EditTagTermsModal
                    type={addModalType}
                    visible
                    onCloseModal={() => {
                        onOpenModal?.();
                        setShowAddModal(false);
                        setTimeout(() => refetch?.(), 2000);
                    }}
                    resources={[
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ]}
                    entityType={entityType}
                    showPropose={entityType === EntityType.Dataset}
                />
            )}
        </>
    );
}
