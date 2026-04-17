interface MetricCardProps {
  value: string;
  label: string;
  description?: string;
  variant?: "excellent" | "good" | "ok" | "slow" | "very-slow" | "neutral";
  animateDelay?: number;
}

export default function MetricCard({
  value,
  label,
  description,
  variant = "neutral",
  animateDelay = 0,
}: MetricCardProps) {
  const variantClass =
    variant !== "neutral" ? `metric-card--${variant}` : "";

  return (
    <div
      class={`metric-card ${variantClass}`}
      data-animate
      style={`--animate-delay: ${animateDelay}s`}
    >
      <div class="metric-card__value">{value}</div>
      <div class="metric-card__label">{label}</div>
      {description && (
        <div class="metric-card__description">{description}</div>
      )}
    </div>
  );
}
