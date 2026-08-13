import type { ComponentChildren } from "preact";

interface StatItem {
  value: string;
  label: string;
}

interface ProductPageProps {
  title: string;
  description: string;
  category: 'language' | 'platform' | 'database' | 'tool';
  status: 'available' | 'in-progress' | 'planned';
  accent?: string;
  heroAccentRgb?: string;
  heroTagline?: string;
  stats?: StatItem[];
  actions?: Array<{ label: string; href: string }>;
  children: ComponentChildren;
}

export default function ProductPage({
  title,
  description,
  category,
  status,
  accent = 'var(--accent)',
  heroAccentRgb = '0,229,160',
  heroTagline,
  stats,
  actions = [],
  children,
}: ProductPageProps) {
  const statusLabel = status === 'available' ? 'Available' : status === 'in-progress' ? 'In progress' : 'Planned';

  return (
    <main id="main-content">
    <article class="product-page">
        <header class="product-header">
          <div class="container container--narrow product-header__inner">
            <div class="product-header__meta" data-animate>
              <span class="product-header__category">{category}</span>
              <span class={`status-badge status-badge--${status}`}>{statusLabel}</span>
            </div>
            <h1 class="product-header__title" data-animate style={{ "--animate-delay": "0.1s" } as any}>{title}</h1>
            <p class="product-header__desc" data-animate style={{ "--animate-delay": "0.15s" } as any}>{description}</p>
            {actions.length > 0 && <div class="product-header__actions" data-animate style={{ "--animate-delay": "0.2s" } as any}>
              {actions.slice(0, 2).map((action, index) => (
                <a href={action.href} class={`btn ${index === 0 ? "btn--primary" : "btn--ghost"}`} key={action.href}>{action.label}</a>
              ))}
            </div>}
          </div>
        </header>

        <div class="product-content">
          <div class="product-content__body">
            {children}
          </div>
        </div>

      </article>
    </main>
  );
}
