import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { appName, gitConfig } from '@/lib/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <div className="flex items-center gap-3">
          <div className="flex h-7 w-7 items-center justify-center rounded-md bg-[var(--chabi-amber)] text-sm font-bold text-black">
            C
          </div>
          <span className="text-lg font-bold tracking-tight">{appName}</span>
        </div>
      ),
    },
    links: [
      {
        text: 'Docs',
        url: '/docs',
      },
      {
        text: 'Benchmarks',
        url: '/docs/benchmarks/methodology',
      },
      {
        text: 'Self Hosting',
        url: '/docs/self-hosting/docker-compose',
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
