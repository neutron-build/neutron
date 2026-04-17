interface FeatureGridProps {
  columns?: 2 | 3;
  accentRgb?: string;
  children: any;
}

export default function FeatureGrid({
  columns = 3,
  accentRgb = "0, 229, 160",
  children,
}: FeatureGridProps) {
  return (
    <div class={`feature-grid feature-grid--cols-${columns}`} data-animate>
      {children}
    </div>
  );
}
