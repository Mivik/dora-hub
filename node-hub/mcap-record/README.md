# mcap-record

Dora data recording using MCAP format.

This nodes is still experimental.

## Getting Started

```bash
cargo install mcap-record --locked
```

## Adding to existing graph:

```yaml
- id: mcap-record
  source: mcap-record
  env:
    - MCAP_FILE: path/to/your/record.mcap  # Optional, default is record.mcap
  inputs:
    image: webcam/image
    text: webcam/text
    # You can add any input and it is going to be logged.
```
