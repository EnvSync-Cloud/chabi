export const appName = 'Chabi';
export const appDescription =
  'Rust Redis-compatible server docs for setup, benchmarks, self-hosting, and operational references.';
export const docsRoute = '/docs';
export const docsImageRoute = '/og/docs';
export const docsContentRoute = '/llms.mdx/docs';
export const defaultSiteUrl = 'https://envsync-cloud.github.io/chabi';
export const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? defaultSiteUrl;

export const gitConfig = {
  user: 'EnvSync-Cloud',
  repo: 'chabi',
  branch: 'main',
};
