import type { ErrorBoundaryProps } from "neutron";

export const config = { mode: "static" };

export async function loader() {
  return {
    message: "This page demonstrates error handling.",
    timestamp: new Date().toISOString(),
  };
}

interface LoaderData {
  message: string;
  timestamp: string;
}

export default function ErrorDemo({ data }: { data: LoaderData }) {
  return (
    <div>
      <h1>Error Handling Demo</h1>
      <p>{data?.message}</p>
      <p><small>Loaded at: {data?.timestamp}</small></p>
      <p style="margin-top: 1rem; color: #888;">
        This page has an ErrorBoundary export that catches errors in loader/render.
      </p>
    </div>
  );
}

export function ErrorBoundary({ error, reset }: ErrorBoundaryProps) {
  return (
    <div style="padding: 2rem; border: 2px solid #FF4444; border-radius: 8px; max-width: 600px;">
      <h1 style="color: #FF4444; margin-top: 0;">Something went wrong</h1>
      <p style="font-size: 1.125rem;">{error.message}</p>
      <details style="margin-top: 1rem;">
        <summary style="cursor: pointer; color: #888;">Stack trace</summary>
        <pre style="font-size: 0.75rem; overflow-x: auto; color: #888; background: #141414; padding: 1rem; border-radius: 4px;">
          {error.stack}
        </pre>
      </details>
      <p style="margin-top: 1.5rem;">
        <a href="/error-demo" style="color: #00E5A0;">Try again</a>
      </p>
    </div>
  );
}
