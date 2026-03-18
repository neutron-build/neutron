import { useRef } from "preact/hooks";

export function CopyCommand() {
  const cmdRef = useRef<HTMLElement>(null);
  const hintRef = useRef<HTMLSpanElement>(null);

  function handleClick() {
    navigator.clipboard.writeText("curl -sL tebian.org/install | bash").then(() => {
      if (hintRef.current) hintRef.current.textContent = "copied!";
      if (cmdRef.current) cmdRef.current.classList.add("copied");
      setTimeout(() => {
        if (hintRef.current) hintRef.current.textContent = "copy";
        if (cmdRef.current) cmdRef.current.classList.remove("copied");
      }, 2000);
    });
  }

  return (
    <code class="arm-cmd" ref={cmdRef} title="Click to copy" onClick={handleClick}>
      curl -sL tebian.org/install | bash
      <span class="copy-hint" ref={hintRef}>copy</span>
    </code>
  );
}
