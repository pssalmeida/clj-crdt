# crdt

CRDTs - Conflict-free Replicated Data Types:
  - Handoff Counters - eventually consistent distributed counters, as
    presented in http://arxiv.org/abs/1307.3207


## Usage

- Handoff Counters - eventually consistent distributed counters

```Clojure
(use crdt.handoff-counter)
(let [c1 (init :id1 tier)
      c2 (init :id2 tier)
      c1 (incr c1)
      c2 (incr c2)
      c1 (join c1 c2)
      c2 (join c2 c1)
      ; ...]
  (value c1))
```

## License

Copyright (C) 2013 Paulo Sérgio Almeida

Distributed under the Eclipse Public License, the same as Clojure.

