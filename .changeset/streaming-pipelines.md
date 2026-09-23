---
"just-bash": minor
---

Add an initial bounded streaming path for simple command pipelines, with chunked command I/O, backpressure, early consumer cancellation, stdin streaming for cat and head, incremental seq output, and incremental single-path rg discovery and search output. Complex shell syntax and unmigrated command implementations retain their existing buffering.
