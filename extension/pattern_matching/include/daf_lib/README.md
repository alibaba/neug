# daf_lib

This directory contains a vendored, modified copy of the **DAF** subgraph
matching library, used by the `pattern_matching` extension for exact matching.

- **Source:** https://github.com/SNUCSE-CTA/DAF
- **License:** MIT (see [`LICENSE`](./LICENSE))
- **Modifications:** Adapted for NeuG (storage adapter, directed-edge checks,
  removal of unused file-based property loading, etc.). Each file carries a
  header comment noting that it originates from the DAF project and who
  modified it.

DAF is described in:

> **Versatile Equivalences: Speeding up Subgraph Query Processing and Subgraph
> Matching** (SIGMOD 2019) — Myoungji Han, Hyunjoon Kim, Geonmo Gu,
> Kunsoo Park, Wook-Shin Han.
