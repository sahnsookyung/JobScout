import React, { useMemo, useState } from 'react';
import { Download, RefreshCw, Wand2 } from 'lucide-react';
import { useMutation } from '@tanstack/react-query';

import { Button } from '@/components/ui/Button';
import { toast } from '@/components/ui/Toast';
import { resumeVariantsApi, type ResumeVariantDownload } from '@/services/resumeVariantsApi';
import type { ResumeVariant, ResumeVariantClaim, ResumeVariantDownloadFormat } from '@/types/api';

type ResumeVariantPanelProps = Readonly<{
    matchId: string;
}>;

const DOWNLOAD_LABELS: Record<ResumeVariantDownloadFormat, string> = {
    markdown: 'Markdown',
    html: 'HTML',
    docx: 'DOCX',
};

function apiErrorMessage(error: unknown): string {
    return error instanceof Error ? error.message : 'Resume draft request failed.';
}

function claimText(claims: ResumeVariantClaim[] | undefined, fallback: string): string {
    return claims?.find((claim) => claim.text.trim())?.text ?? fallback;
}

function evidenceLabel(variant: ResumeVariant): string {
    const count = variant.evidence_map?.claim_count;
    const claimCount = typeof count === 'number' ? count : null;
    const sources = Array.isArray(variant.evidence_map?.source_types)
        ? variant.evidence_map.source_types.filter((source): source is string => typeof source === 'string')
        : [];

    const claimLabel = claimCount === null ? 'Sourced claims' : `${claimCount} sourced claim${claimCount === 1 ? '' : 's'}`;
    return sources.length > 0 ? `${claimLabel} from ${sources.join(', ')}` : claimLabel;
}

function startBrowserDownload(download: ResumeVariantDownload): void {
    const url = URL.createObjectURL(download.blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = download.filename;
    document.body.appendChild(anchor);
    try {
        anchor.click();
    } finally {
        anchor.remove();
        URL.revokeObjectURL(url);
    }
}

export const ResumeVariantPanel: React.FC<ResumeVariantPanelProps> = ({ matchId }) => {
    const [variant, setVariant] = useState<ResumeVariant | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [downloadingFormat, setDownloadingFormat] = useState<ResumeVariantDownloadFormat | null>(null);

    const createMutation = useMutation({
        mutationFn: (force: boolean) =>
            resumeVariantsApi.createForMatch(matchId, {
                template_key: 'compact',
                tone: 'concise',
                force,
            }),
        onSuccess: (response) => {
            const nextVariant = response.data.variant;
            setVariant(nextVariant);
            setError(null);
            toast.success(nextVariant.reused ? 'Resume draft already current.' : 'Resume draft generated.');
        },
        onError: (mutationError) => {
            const message = apiErrorMessage(mutationError);
            setError(message);
            toast.error(message);
        },
    });

    const downloadMutation = useMutation({
        mutationFn: (format: ResumeVariantDownloadFormat) => {
            if (!variant) {
                throw new Error('Generate a resume draft before downloading.');
            }
            return resumeVariantsApi.downloadVariant(variant.id, format);
        },
        onSuccess: (download) => {
            startBrowserDownload(download);
        },
        onError: (mutationError) => {
            toast.error(apiErrorMessage(mutationError));
        },
        onSettled: () => {
            setDownloadingFormat(null);
        },
    });

    const visibleDownloads = useMemo(
        () => variant?.download_formats.filter((format) => format in DOWNLOAD_LABELS) ?? [],
        [variant],
    );
    const summary = variant ? claimText(variant.content.summary, 'No summary text generated.') : null;
    const targetedEvidence = variant ? claimText(variant.content.targeted_evidence, 'No targeted evidence generated.') : null;
    const skillLabels = variant?.content.skills?.map((skill) => skill.text).filter(Boolean).slice(0, 8) ?? [];
    const gapLabels = variant?.content.gaps?.map((gap) => gap.text).filter(Boolean).slice(0, 6) ?? [];
    const sourceQuality = variant?.content.source_quality;
    const isGenerating = createMutation.isPending;

    const handleGenerate = () => {
        createMutation.mutate(Boolean(variant));
    };

    const handleDownload = (format: ResumeVariantDownloadFormat) => {
        setDownloadingFormat(format);
        downloadMutation.mutate(format);
    };

    return (
        <section aria-labelledby="resume-variant-title">
            <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                    <p className="caption">Resume draft</p>
                    <h4 id="resume-variant-title" className="mt-1 text-[18px] font-medium text-ink">
                        Tailored version for this job
                    </h4>
                </div>
                <Button
                    type="button"
                    variant={variant ? 'secondary' : 'primary'}
                    size="sm"
                    isLoading={isGenerating}
                    onClick={handleGenerate}
                >
                    {variant ? <RefreshCw className="h-3.5 w-3.5" aria-hidden="true" /> : <Wand2 className="h-3.5 w-3.5" aria-hidden="true" />}
                    {variant ? 'Regenerate' : 'Generate draft'}
                </Button>
            </div>

            {error && (
                <p role="alert" className="mt-4 border border-warn/40 bg-warn-soft px-4 py-3 text-[13px] text-ink">
                    {error}
                </p>
            )}

            <div className="mt-5 divide-y divide-rule border border-rule bg-surface">
                {!variant ? (
                    <div className="px-5 py-8 text-[14px] text-ink-soft">
                        No resume draft for this match yet.
                    </div>
                ) : (
                    <>
                        <div className="grid gap-5 px-5 py-5 md:grid-cols-[1.3fr_0.7fr]">
                            <div>
                                <p className="caption">Preview</p>
                                <p className="mt-2 text-[14px] leading-relaxed text-ink">{summary}</p>
                                <p className="mt-3 text-[13px] leading-relaxed text-ink-soft">{targetedEvidence}</p>
                            </div>
                            <div>
                                <p className="caption">Evidence</p>
                                <p className="mt-2 text-[13px] leading-relaxed text-ink-soft">
                                    {evidenceLabel(variant)}
                                </p>
                                {skillLabels.length > 0 && (
                                    <div className="mt-3 flex flex-wrap gap-1.5">
                                        {skillLabels.map((skill) => (
                                            <span key={skill} className="border border-rule bg-surface-sunk px-2 py-1 text-[12px] text-ink-soft">
                                                {skill}
                                            </span>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>

                        {(gapLabels.length > 0 || sourceQuality) && (
                            <div className="grid gap-5 px-5 py-4 md:grid-cols-2">
                                {gapLabels.length > 0 && (
                                    <div>
                                        <p className="caption">Not claimed</p>
                                        <ul className="mt-2 space-y-1 text-[13px] leading-5 text-ink-soft">
                                            {gapLabels.map((gap) => (
                                                <li key={gap}>{gap}</li>
                                            ))}
                                        </ul>
                                    </div>
                                )}
                                {sourceQuality && (
                                    <div>
                                        <p className="caption">Source quality</p>
                                        <dl className="mt-2 grid gap-1.5 text-[13px] text-ink-soft">
                                            <div className="flex justify-between gap-4">
                                                <dt>Description</dt>
                                                <dd className="text-ink">{sourceQuality.job_description_completeness ?? 'unknown'}</dd>
                                            </div>
                                            <div className="flex justify-between gap-4">
                                                <dt>Source</dt>
                                                <dd className="text-ink">{sourceQuality.job_description_source ?? 'unknown'}</dd>
                                            </div>
                                            {sourceQuality.job_description_warning_code && (
                                                <div className="flex justify-between gap-4">
                                                    <dt>Warning</dt>
                                                    <dd className="text-ink">{sourceQuality.job_description_warning_code}</dd>
                                                </div>
                                            )}
                                        </dl>
                                    </div>
                                )}
                            </div>
                        )}

                        {variant.warnings.length > 0 && (
                            <div className="px-5 py-4">
                                <p className="caption">Warnings</p>
                                <ul className="mt-2 space-y-1 text-[13px] text-ink-soft">
                                    {variant.warnings.map((warning) => (
                                        <li key={warning}>{warning}</li>
                                    ))}
                                </ul>
                            </div>
                        )}

                        <div className="flex flex-wrap items-center justify-between gap-3 px-5 py-4">
                            <p className="text-[13px] text-ink-muted">
                                {variant.reused ? 'Current draft reused.' : 'Generated now.'}
                            </p>
                            <div className="flex flex-wrap gap-2">
                                {visibleDownloads.map((format) => (
                                    <Button
                                        key={format}
                                        type="button"
                                        variant="secondary"
                                        size="sm"
                                        isLoading={downloadingFormat === format && downloadMutation.isPending}
                                        onClick={() => handleDownload(format)}
                                    >
                                        <Download className="h-3.5 w-3.5" aria-hidden="true" />
                                        Download {DOWNLOAD_LABELS[format]}
                                    </Button>
                                ))}
                            </div>
                        </div>
                    </>
                )}
            </div>
        </section>
    );
};
