// @ts-nocheck
import { browser } from 'fumadocs-mdx/runtime/browser';
import type * as Config from '../source.config';

const create = browser<typeof Config, import("fumadocs-mdx/runtime/types").InternalTypeConfig & {
  DocData: {
  }
}>();
const browserCollections = {
  docs: create.doc("docs", {"index.mdx": () => import("../content/docs/index.mdx?collection=docs"), "benchmarks/methodology.mdx": () => import("../content/docs/benchmarks/methodology.mdx?collection=docs"), "benchmarks/results.mdx": () => import("../content/docs/benchmarks/results.mdx?collection=docs"), "getting-started/installation.mdx": () => import("../content/docs/getting-started/installation.mdx?collection=docs"), "getting-started/quickstart.mdx": () => import("../content/docs/getting-started/quickstart.mdx?collection=docs"), "development/architecture.mdx": () => import("../content/docs/development/architecture.mdx?collection=docs"), "operations/configuration.mdx": () => import("../content/docs/operations/configuration.mdx?collection=docs"), "operations/http-and-metrics.mdx": () => import("../content/docs/operations/http-and-metrics.mdx?collection=docs"), "operations/persistence-and-snapshots.mdx": () => import("../content/docs/operations/persistence-and-snapshots.mdx?collection=docs"), "reference/feature-matrix.mdx": () => import("../content/docs/reference/feature-matrix.mdx?collection=docs"), "reference/supported-commands.mdx": () => import("../content/docs/reference/supported-commands.mdx?collection=docs"), "self-hosting/docker-compose.mdx": () => import("../content/docs/self-hosting/docker-compose.mdx?collection=docs"), }),
};
export default browserCollections;