# tailsurf

## 0.2.0

### Minor Changes

- da3ec78: Make `transcript`, `bytes`, and `terminal` immutable stream kinds. Remove per-record formats from REST, SSE, and WebSocket records. Add the stream kind to WebSocket readiness handshakes.
  
  Add terminal event codecs, canonical stream URL builders, and separate terminal input and output WebSocket clients.

### Patch Changes

- Updated dependencies [da3ec78]
  - @tailsurf/client@0.5.0

## 0.1.1

### Patch Changes

- Updated dependencies [9512b52]
  - @tailsurf/client@0.4.0

## 0.1.0

### Minor Changes

- 00ff17c: Publish `tailsurf` as an alias package that re-exports `@tailsurf/client`.

### Patch Changes

- Updated dependencies [3a2b451]
  - @tailsurf/client@0.3.0
