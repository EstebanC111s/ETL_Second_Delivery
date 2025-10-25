```mermaid
flowchart LR
  A[File 1 (data/input/old.csv)] --> C[Extract]
  B[File 2 (data/input/new.csv)] --> C[Extract]
  AP[Public API] --> C[Extract]
  C --> T[Transform (clean merge)]
  T --> V[Validate (GE)]
  V --> L[Load (dim/fact)]
  L --> D[(DB)]
```
