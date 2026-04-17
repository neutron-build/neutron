interface CodeBlockProps {
  filename?: string;
  annotation?: string;
  children: any;
}

export default function CodeBlock({
  filename,
  annotation,
  children,
}: CodeBlockProps) {
  return (
    <div class="code-block">
      {filename && (
        <div class="code-block__header">
          <span class="code-block__filename">{filename}</span>
        </div>
      )}
      <div class="code-block__body">{children}</div>
      {annotation && (
        <div class="code-block__annotation">{annotation}</div>
      )}
    </div>
  );
}
