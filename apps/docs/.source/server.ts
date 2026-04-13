// @ts-nocheck
import * as __fd_glob_18 from "../content/docs/self-hosting/docker-compose.mdx?collection=docs"
import * as __fd_glob_17 from "../content/docs/reference/supported-commands.mdx?collection=docs"
import * as __fd_glob_16 from "../content/docs/reference/feature-matrix.mdx?collection=docs"
import * as __fd_glob_15 from "../content/docs/operations/persistence-and-snapshots.mdx?collection=docs"
import * as __fd_glob_14 from "../content/docs/operations/http-and-metrics.mdx?collection=docs"
import * as __fd_glob_13 from "../content/docs/operations/configuration.mdx?collection=docs"
import * as __fd_glob_12 from "../content/docs/development/architecture.mdx?collection=docs"
import * as __fd_glob_11 from "../content/docs/getting-started/quickstart.mdx?collection=docs"
import * as __fd_glob_10 from "../content/docs/getting-started/installation.mdx?collection=docs"
import * as __fd_glob_9 from "../content/docs/benchmarks/results.mdx?collection=docs"
import * as __fd_glob_8 from "../content/docs/benchmarks/methodology.mdx?collection=docs"
import * as __fd_glob_7 from "../content/docs/index.mdx?collection=docs"
import { default as __fd_glob_6 } from "../content/docs/self-hosting/meta.json?collection=docs"
import { default as __fd_glob_5 } from "../content/docs/reference/meta.json?collection=docs"
import { default as __fd_glob_4 } from "../content/docs/operations/meta.json?collection=docs"
import { default as __fd_glob_3 } from "../content/docs/getting-started/meta.json?collection=docs"
import { default as __fd_glob_2 } from "../content/docs/development/meta.json?collection=docs"
import { default as __fd_glob_1 } from "../content/docs/benchmarks/meta.json?collection=docs"
import { default as __fd_glob_0 } from "../content/docs/meta.json?collection=docs"
import { server } from 'fumadocs-mdx/runtime/server';
import type * as Config from '../source.config';

const create = server<typeof Config, import("fumadocs-mdx/runtime/types").InternalTypeConfig & {
  DocData: {
  }
}>({"doc":{"passthroughs":["extractedReferences"]}});

export const docs = await create.docs("docs", "content/docs", {"meta.json": __fd_glob_0, "benchmarks/meta.json": __fd_glob_1, "development/meta.json": __fd_glob_2, "getting-started/meta.json": __fd_glob_3, "operations/meta.json": __fd_glob_4, "reference/meta.json": __fd_glob_5, "self-hosting/meta.json": __fd_glob_6, }, {"index.mdx": __fd_glob_7, "benchmarks/methodology.mdx": __fd_glob_8, "benchmarks/results.mdx": __fd_glob_9, "getting-started/installation.mdx": __fd_glob_10, "getting-started/quickstart.mdx": __fd_glob_11, "development/architecture.mdx": __fd_glob_12, "operations/configuration.mdx": __fd_glob_13, "operations/http-and-metrics.mdx": __fd_glob_14, "operations/persistence-and-snapshots.mdx": __fd_glob_15, "reference/feature-matrix.mdx": __fd_glob_16, "reference/supported-commands.mdx": __fd_glob_17, "self-hosting/docker-compose.mdx": __fd_glob_18, });