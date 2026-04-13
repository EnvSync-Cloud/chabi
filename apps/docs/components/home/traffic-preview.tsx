'use client';

import { useEffect, useState } from 'react';

const clients = [
  { name: 'api-1', tone: 'amber' },
  { name: 'worker-2', tone: 'teal' },
  { name: 'web-3', tone: 'blue' },
  { name: 'queue-4', tone: 'violet' },
] as const;

type ClientName = (typeof clients)[number]['name'];

type PreviewFrame = {
  hitRate: string;
  missRate: string;
  sampledReads: string;
  activeClients: ClientName[];
  events: Array<{
    command: string;
    status: 'hit' | 'miss' | 'write' | 'snapshot';
    detail: string;
  }>;
};

const frames: PreviewFrame[] = [
  {
    hitRate: '92.4%',
    missRate: '7.6%',
    sampledReads: '1.8k/s',
    activeClients: ['api-1', 'worker-2', 'web-3'],
    events: [
      { command: 'GET session:42', status: 'hit', detail: 'served from memory' },
      { command: 'SET feed:home', status: 'write', detail: 'client stores refreshed value' },
      { command: 'SAVE', status: 'snapshot', detail: 'persisted to /data/chabi.kdb' },
    ],
  },
  {
    hitRate: '91.8%',
    missRate: '8.2%',
    sampledReads: '1.9k/s',
    activeClients: ['api-1', 'worker-2', 'queue-4'],
    events: [
      { command: 'GET profile:8', status: 'hit', detail: 'hot key returned' },
      { command: 'GET timeline:77', status: 'miss', detail: 'client fetches origin data' },
      { command: 'SET timeline:77', status: 'write', detail: 'cache repopulated after miss' },
      { command: 'SAVE', status: 'snapshot', detail: 'snapshot interval completed' },
    ],
  },
  {
    hitRate: '93.1%',
    missRate: '6.9%',
    sampledReads: '2.1k/s',
    activeClients: ['api-1', 'web-3', 'queue-4'],
    events: [
      { command: 'GET tenant:7', status: 'hit', detail: 'cache served directly' },
      { command: 'SET inbox:101', status: 'write', detail: 'new key inserted into cache' },
      { command: 'SAVE', status: 'snapshot', detail: 'disk state remains current' },
    ],
  },
];

export function TrafficPreview() {
  const [frameIndex, setFrameIndex] = useState(0);

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setFrameIndex((current) => (current + 1) % frames.length);
    }, 1800);

    return () => window.clearInterval(intervalId);
  }, []);

  const frame = frames[frameIndex];

  return (
      <div className="hero-visual__content">
        <div className="hero-visual__header">
        <span className="hero-visual__eyebrow">Live traffic preview</span>
        </div>

      <div className="hero-live-row" aria-label="Dynamic cache preview">
        <div className="live-chip" data-tone="hit">
          <span>Hit rate</span>
          <strong>{frame.hitRate}</strong>
        </div>
        <div className="live-chip" data-tone="miss">
          <span>Miss rate</span>
          <strong>{frame.missRate}</strong>
        </div>
        <div className="live-chip" data-tone="neutral">
          <span>Sampled reads</span>
          <strong>{frame.sampledReads}</strong>
        </div>
      </div>

      <div className="flow-diagram">
        <div className="flow-column">
          <span className="flow-column__title">Clients</span>
          <div className="hero-node-row">
            {clients.map((client, index) => (
              <div
                key={client.name}
                className="client-node"
                data-tone={client.tone}
                data-active={frame.activeClients.includes(client.name)}
                style={{ animationDelay: `${index * 140}ms` }}
              >
                <span className="signal-dot" />
                <span className="client-node__name">{client.name}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="flow-connector" aria-hidden="true" />

        <div className="flow-column flow-column--core">
          <span className="flow-column__title">Cache</span>
          <div className="cache-core">
            <div className="request-log" aria-live="polite">
              {frame.events.map((event) => (
                <div key={`${frame.hitRate}-${event.command}`} className="request-log__row">
                  <code>{event.command}</code>
                  <span className="request-log__status" data-tone={event.status}>
                    {event.status}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="disk-panel">
        <span className="lane__label">Snapshot / Disk</span>
        <strong>Persisted state</strong>
        <code>/data/chabi.kdb</code>
      </div>
    </div>
  );
}
