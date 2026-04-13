import Link from 'next/link';
import { TrafficPreview } from '@/components/home/traffic-preview';

const trustItems = ['RESP compatible', 'Snapshot persistence', 'Prometheus metrics', 'Docker self-hosting'];

const featurePanels = [
  {
    title: 'Traffic path',
    eyebrow: 'Client Flow',
    body: 'Multiple Redis-compatible clients converge on one cache layer, with clear separation between fast cache hits and slower miss paths that need a deeper read or restore path.',
    bullets: ['Multiple clients, one cache edge', 'Hit and miss behavior is visible', 'Redis-style request flow stays central'],
  },
  {
    title: 'Operate it',
    eyebrow: 'Observability',
    body: 'Chabi keeps the operational surface small and explicit: health on `/`, Prometheus text on `/metrics`, and in-memory snapshot visibility on `/snapshot`.',
    bullets: ['Prometheus-friendly metrics', 'Health and snapshot inspection', 'Inspectable without extra services'],
  },
  {
    title: 'Persist it',
    eyebrow: 'State',
    body: 'Snapshots are part of the runtime story, not an afterthought. Mount `/data`, set an interval, and keep the path from memory to disk straightforward.',
    bullets: ['Configurable snapshot path', 'Timed background persistence', 'Docker-first self-hosting path'],
  },
] as const;

export default function HomePage() {
  return (
    <main className="relative mx-auto flex w-full max-w-7xl flex-1 flex-col px-4 pb-20 pt-10 sm:px-6 lg:px-8">
      <section className="hero-grid">
        <div className="hero-copy">
          <span className="hero-eyebrow">Redis-Compatible Cache Server</span>
          <h1 className="hero-title">See client traffic turn into cache hits, misses, and persisted state.</h1>
          <p className="hero-body">
            Chabi sits between multiple clients and your cached data, exposing the request path, metrics
            surface, and snapshot behavior you need to run it with confidence.
          </p>

          <div className="hero-actions">
            <Link href="/docs" className="hero-action hero-action--primary">
              Read The Docs
            </Link>
            <Link href="/docs/self-hosting/docker-compose" className="hero-action hero-action--secondary">
              Self Host Chabi
            </Link>
            <Link href="/docs/benchmarks/results" className="hero-link">
              View Benchmarks
            </Link>
          </div>

          <div className="trust-strip" aria-label="Chabi capabilities">
            {trustItems.map((item) => (
              <span key={item} className="trust-pill">
                {item}
              </span>
            ))}
          </div>
        </div>

        <div className="hero-visual" aria-label="Illustration of multiple clients connecting to Chabi cache">
          <div className="visual-grid" />
          <TrafficPreview />
        </div>
      </section>

      <section className="home-section">
        <div className="home-section__header">
          <span className="home-section__eyebrow">Why engineers evaluate Chabi</span>
          <h2 className="home-section__title">A cache server that reads like infrastructure, not just branding.</h2>
        </div>

        <div className="home-panel-grid">
          {featurePanels.map((panel, index) => (
            <article key={panel.title} className="home-panel" style={{ animationDelay: `${index * 120}ms` }}>
              <span className="home-panel__eyebrow">{panel.eyebrow}</span>
              <h3 className="home-panel__title">{panel.title}</h3>
              <p className="home-panel__body">{panel.body}</p>
              <ul className="home-panel__list">
                {panel.bullets.map((bullet) => (
                  <li key={bullet}>{bullet}</li>
                ))}
              </ul>
            </article>
          ))}
        </div>
      </section>

      <section className="ops-grid">
        <article className="ops-panel">
          <div className="ops-panel__header">
            <span className="ops-panel__eyebrow">Benchmarks</span>
            <span className="ops-panel__chip">Hetzner CCX23</span>
          </div>
          <h2 className="ops-panel__title">Published throughput stays visible from the landing page.</h2>
          <p className="ops-panel__body">
            Current published comparison: Chabi <strong>35633 ops/sec</strong>, Redis <strong>12267</strong>,
            DiceDB <strong>15655</strong>.
          </p>
          <Link href="/docs/benchmarks/results" className="ops-panel__link">
            Open Benchmark Results
          </Link>
        </article>

        <article className="ops-panel">
          <div className="ops-panel__header">
            <span className="ops-panel__eyebrow">Self Hosting</span>
            <span className="ops-panel__chip">Docker Compose</span>
          </div>
          <h2 className="ops-panel__title">Container-first deployment with an explicit persistence path.</h2>
          <p className="ops-panel__body">
            Start with <code>ghcr.io/envsync-cloud/chabi:latest</code>, expose Redis and HTTP, and mount
            <code> /data</code> for snapshots.
          </p>
          <Link href="/docs/self-hosting/docker-compose" className="ops-panel__link">
            Open The Compose Guide
          </Link>
        </article>
      </section>
    </main>
  );
}
