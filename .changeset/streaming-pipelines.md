---
"just-bash": minor
---

Add an initial bounded streaming path for simple command pipelines, with chunked command I/O, backpressure, early consumer cancellation, and stdin streaming for cat, head, and seq. Complex shell syntax and unmigrated command implementations retain their existing buffering.
