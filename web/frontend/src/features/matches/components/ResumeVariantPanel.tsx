import React, { useMemo, useState } from 'react';
import { Download, RefreshCw, Wand2 } from 'lucide-react';
import { useMutation } from '@tanstack/react-query';

import { Button } from '@/components/ui/Button';
import { toast } from '@/components/ui/Toast';
import { resumeVariantsApi, type ResumeVariantDownload } from '@/services/resumeVariantsApi';
import type { ResumeVariant, ResumeVariantDownloadFormat } from '@/types/api';

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
    const summaryClaims = variant?.content.summary?.filter((claim) => claim.text.trim()) ?? [];
    const targetedEvidenceClaims = variant?.content.targeted_evidence?.filter((claim) => claim.text.trim()) ?? [];
    const skillLabels = variant?.content.skills?.map((skill) => skill.text).filter(Boolean).slice(0, 8) ?? [];
    const contactParts = variant
        ? [
            variant.content.contact?.email,
            variant.content.contact?.phone,
            variant.content.contact?.location,
            ...(variant.content.contact?.links ?? []),
        ].filter((value): value is string => Boolean(value))
        : [];
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
                                <article className="mt-3 max-h-[36rem] overflow-y-auto border border-rule bg-white px-5 py-5 text-ink">
                                    <h5 className="text-[20px] font-semibold">
                                        {variant.content.contact?.name || 'Resume'}
                                    </h5>
                                    {contactParts.length > 0 && (
                                        <p className="mt-1 text-[12px] leading-5 text-ink-soft">
                                            {contactParts.join(' · ')}
                                        </p>
                                    )}

                                    <h6 className="mt-5 border-b border-rule pb-1 text-[13px] font-semibold uppercase tracking-wide">
                                        Professional Summary
                                    </h6>
                                    {summaryClaims.length > 0 ? summaryClaims.map((claim, index) => (
                                        <p key={`${index}-${claim.text}`} className="mt-2 text-[13px] leading-relaxed">
                                            {claim.text}
                                        </p>
                                    )) : (
                                        <p className="mt-2 text-[13px] text-ink-soft">No summary text generated.</p>
                                    )}

                                    {variant.content.skills && variant.content.skills.length > 0 && (
                                        <>
                                            <h6 className="mt-5 border-b border-rule pb-1 text-[13px] font-semibold uppercase tracking-wide">
                                                Skills
                                            </h6>
                                            <p className="mt-2 text-[13px] leading-relaxed">
                                                {variant.content.skills.map((skill) => skill.text).filter(Boolean).join(', ')}
                                            </p>
                                        </>
                                    )}

                                    {variant.content.experience && variant.content.experience.length > 0 && (
                                        <>
                                            <h6 className="mt-5 border-b border-rule pb-1 text-[13px] font-semibold uppercase tracking-wide">
                                                Experience
                                            </h6>
                                            <div className="space-y-4">
                                                {variant.content.experience.map((entry, index) => (
                                                    <section key={entry.entry_id || `${entry.company}-${entry.title}-${index}`} className="mt-3">
                                                        <div className="flex flex-wrap justify-between gap-x-4 gap-y-1">
                                                            <p className="text-[13px] font-semibold">
                                                                {[entry.title, entry.company].filter(Boolean).join(' — ')}
                                                            </p>
                                                            <p className="text-[12px] text-ink-soft">
                                                                {[entry.start_date, entry.end_date].filter(Boolean).join(' – ')}
                                                            </p>
                                                        </div>
                                                        {entry.bullets && entry.bullets.length > 0 && (
                                                            <ul className="mt-1 list-disc space-y-1 pl-5 text-[13px] leading-relaxed">
                                                                {entry.bullets.map((bullet, bulletIndex) => (
                                                                    <li key={`${bulletIndex}-${bullet.text}`}>{bullet.text}</li>
                                                                ))}
                                                            </ul>
                                                        )}
                                                    </section>
                                                ))}
                                            </div>
                                        </>
                                    )}

                                    {variant.content.projects && variant.content.projects.length > 0 && (
                                        <>
                                            <h6 className="mt-5 border-b border-rule pb-1 text-[13px] font-semibold uppercase tracking-wide">
                                                Projects
                                            </h6>
                                            {variant.content.projects.map((project, index) => (
                                                <section key={project.entry_id || `${project.name}-${index}`} className="mt-3">
                                                    <p className="text-[13px] font-semibold">{project.name}</p>
                                                    {project.technologies && project.technologies.length > 0 && (
                                                        <p className="text-[12px] text-ink-soft">{project.technologies.join(', ')}</p>
                                                    )}
                                                    {project.bullets && project.bullets.length > 0 && (
                                                        <ul className="mt-1 list-disc space-y-1 pl-5 text-[13px] leading-relaxed">
                                                            {project.bullets.map((bullet, bulletIndex) => (
                                                                <li key={`${bulletIndex}-${bullet.text}`}>{bullet.text}</li>
                                                            ))}
                                                        </ul>
                                                    )}
                                                </section>
                                            ))}
                                        </>
                                    )}

                                    {variant.content.education && variant.content.education.length > 0 && (
                                        <>
                                            <h6 className="mt-5 border-b border-rule pb-1 text-[13px] font-semibold uppercase tracking-wide">
                                                Education
                                            </h6>
                                            {variant.content.education.map((entry, index) => (
                                                <p key={`${entry.institution}-${entry.degree}-${index}`} className="mt-2 text-[13px]">
                                                    {[entry.degree, entry.field_of_study, entry.institution, entry.graduation_year]
                                                        .filter(Boolean)
                                                        .join(' — ')}
                                                </p>
                                            ))}
                                        </>
                                    )}
                                </article>
                            </div>
                            <div>
                                <p className="caption">Evidence</p>
                                <p className="mt-2 text-[13px] leading-relaxed text-ink-soft">
                                    {evidenceLabel(variant)}
                                </p>
                                {variant.content.generation?.tailored && (
                                    <p className="mt-2 text-[12px] leading-5 text-ink-soft">
                                        Tailored by {variant.content.generation.provider || 'configured provider'}
                                        {variant.content.generation.model ? ` · ${variant.content.generation.model}` : ''}
                                    </p>
                                )}
                                {targetedEvidenceClaims.length > 0 && (
                                    <ul className="mt-3 list-disc space-y-1 pl-4 text-[12px] leading-5 text-ink-soft">
                                        {targetedEvidenceClaims.slice(0, 5).map((claim, index) => (
                                            <li key={`${index}-${claim.text}`}>{claim.text}</li>
                                        ))}
                                    </ul>
                                )}
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
