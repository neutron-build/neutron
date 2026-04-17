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
  children,
}: ProductPageProps) {
  const statusLabel = status === 'available' ? 'Available' : status === 'in-progress' ? 'In Progress' : 'Coming Soon';

  return (
    <main id="main-content">
    <article class="product-page">
        <header class="product-header">
          <div class="container container--narrow product-header__inner">
            <div class="product-header__category" data-animate>{category}</div>
            <h1 class="product-header__title" data-animate style={{ "--animate-delay": "0.1s" } as any}>{title}</h1>
            <p class="product-header__desc" data-animate style={{ "--animate-delay": "0.15s" } as any}>{description}</p>
            {heroTagline && (
              <p class="product-header__tagline" data-animate style={{ "--animate-delay": "0.18s" } as any}>{heroTagline}</p>
            )}
            <div class="product-header__status" data-animate style={{ "--animate-delay": "0.2s" } as any}>
              <span class={`status-badge status-badge--${status}`}>
                {statusLabel}
              </span>
            </div>
            {stats && stats.length > 0 && (
              <div class="product-header__stats" data-animate style={{ "--animate-delay": "0.25s" } as any}>
                {stats.map((stat) => (
                  <div class="stat-pill" key={stat.label}>
                    <span class="stat-pill__value">{stat.value}</span>
                    <span class="stat-pill__label">{stat.label}</span>
                  </div>
                ))}
              </div>
            )}
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
