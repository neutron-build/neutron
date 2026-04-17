interface SectionBreakProps {
  accentRgb?: string;
}

export default function SectionBreak({
  accentRgb = "0, 229, 160",
}: SectionBreakProps) {
  return <div class="section-break" aria-hidden="true"></div>;
}
