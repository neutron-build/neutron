import type { ComponentChildren } from "preact";

export type CalloutType = "note" | "warning" | "tip" | "danger";

export interface CalloutProps {
  type: CalloutType;
  title?: string;
  children: ComponentChildren;
}

const icons: Record<CalloutType, string> = {
  note: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>`,
  warning: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
  tip: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18h6"/><path d="M10 22h4"/><path d="M12 2a7 7 0 014 12.7V17H8v-2.3A7 7 0 0112 2z"/></svg>`,
  danger: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="7.86 2 16.14 2 22 7.86 22 16.14 16.14 22 7.86 22 2 16.14 2 7.86 7.86 2"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>`,
};

const defaultTitles: Record<CalloutType, string> = {
  note: "Note",
  warning: "Warning",
  tip: "Tip",
  danger: "Danger",
};

export default function Callout(props: CalloutProps) {
  const title = props.title ?? defaultTitles[props.type];

  return (
    <aside class={`callout callout-${props.type}`}>
      <span
        class="callout-icon"
        dangerouslySetInnerHTML={{ __html: icons[props.type] }}
      />
      <div class="callout-content">
        {title && <p class="callout-title">{title}</p>}
        {props.children}
      </div>
    </aside>
  );
}
