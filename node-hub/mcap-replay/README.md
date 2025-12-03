# mcap-replay

Dora data replaying using MCAP format.

This nodes is still experimental.

## Getting Started

```bash
cargo install mcap-replay --locked
```

## Adding to existing graph:

```yaml
- id: mcap-replay
  path: mcap-replay
  env:
    - MCAP_FILE: path/to/your/record.mcap  # Optional, default is record.mcap
  outputs:
    - image
    - text
    # You can add any output and it is going to be logged.
```
