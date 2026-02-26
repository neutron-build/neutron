import { Component, h, ComponentType } from "preact";

export interface ErrorBoundaryFallbackProps {
  error: Error;
  reset: () => void;
}

interface ClientErrorBoundaryProps {
  fallback?: ComponentType<{ error: Error; reset?: () => void }>;
  children?: preact.ComponentChildren;
}

interface ClientErrorBoundaryState {
  error: Error | null;
}

export class ClientErrorBoundary extends Component<ClientErrorBoundaryProps, ClientErrorBoundaryState> {
  state: ClientErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ClientErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error) {
    console.error("[neutron] Route error:", error);
  }

  reset = () => {
    this.setState({ error: null });
  };

  render() {
    const { error } = this.state;

    if (error) {
      const Fallback = this.props.fallback;
      if (Fallback) {
        return h(Fallback, { error, reset: this.reset });
      }
      return h(DefaultErrorFallback, { error, reset: this.reset });
    }

    return this.props.children;
  }
}

function DefaultErrorFallback({ error, reset }: ErrorBoundaryFallbackProps) {
  const isDev = typeof import.meta !== "undefined" && import.meta.env?.DEV;

  return h(
    "div",
    {
      style:
        "padding: 2rem; font-family: system-ui, -apple-system, sans-serif; max-width: 600px; margin: 2rem auto;",
    },
    h(
      "h1",
      { style: "font-size: 1.5rem; margin: 0 0 1rem;" },
      "Something went wrong"
    ),
    isDev
      ? h(
          "pre",
          {
            style:
              "background: #fef2f2; color: #991b1b; padding: 1rem; border-radius: 0.5rem; white-space: pre-wrap; word-break: break-word; font-size: 0.875rem; overflow: auto;",
          },
          error.stack || error.message
        )
      : h(
          "p",
          { style: "color: #6b7280;" },
          "An unexpected error occurred."
        ),
    h(
      "button",
      {
        onClick: reset,
        style:
          "margin-top: 1rem; padding: 0.5rem 1rem; background: #3b82f6; color: white; border: none; border-radius: 0.375rem; cursor: pointer; font-size: 0.875rem;",
      },
      "Try again"
    )
  );
}
