# zippy-openctp

Python-first OpenCTP market data plugin for `zippy`.

## Scope

This repository is the standalone plugin home for OpenCTP market data support.

The bootstrap stage provides:

- an independent git repository
- a Rust workspace skeleton
- a Python package skeleton
- reserved module boundaries for schema, source, normalization, and metrics

It does not yet implement a live OpenCTP market data connection.
