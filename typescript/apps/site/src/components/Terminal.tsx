interface TerminalProps {
  command: string;
}

export default function Terminal({ command }: TerminalProps) {
  return (
    <div class="terminal" data-command={command}>
      <div class="terminal__header">
        <span class="terminal__dot terminal__dot--red"></span>
        <span class="terminal__dot terminal__dot--yellow"></span>
        <span class="terminal__dot terminal__dot--green"></span>
      </div>
      <div class="terminal__body">
        <span class="terminal__prompt">$</span>
        <span class="terminal__text">{command}</span>
        <span class="terminal__cursor"></span>
      </div>
      <button class="terminal__copy" aria-label="Copy command" title="Copy to clipboard">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <rect x="5" y="5" width="9" height="9" rx="1.5" stroke="currentColor" stroke-width="1.5" />
          <path d="M3 10.5V3a1.5 1.5 0 0 1 1.5-1.5H11" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" />
        </svg>
      </button>
    </div>
  );
}
