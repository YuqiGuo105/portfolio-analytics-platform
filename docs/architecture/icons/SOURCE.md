# Architecture Diagram Icons

The brand marks in this directory are pinned from `simple-icons@15.15.0`.
They are copied into the repository so architecture diagrams render and can be
regenerated without a CDN or runtime network dependency.

To update them:

1. Review the upstream Simple Icons release and brand guidelines.
2. Replace only the required SVG files.
3. Update each diagram's `brand` and `brandColor` fields if needed.
4. Regenerate the SVG with `scripts/render-architecture-diagram.mjs`.
5. Inspect the rendered README at desktop and narrow widths.

Simple Icons project: <https://simpleicons.org/>

`elasticsearch.svg` is the official multicolor Elastic mark pinned from
`@elastic/eui@97.3.0`; it is retained in the shared architecture icon set so
all subsystem diagrams can be regenerated with one visual vocabulary.
