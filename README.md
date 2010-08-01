# cascading-clojure
## An idiomatic Clojure wrapper for [Cascading](http://cascading.org).

cascading-clojure wraps much of the verbose java plumbing but preserves the spirit of the cascading API.

It handles serializing and deserailizing all data between clojure function calss injected into individual Cascading operators, as well as allowing arbitrary dynmaic Fields in Cascading Tuples via Clojure maps.

[TODO: need examples]

## Hacking

Get [Leiningen](http://github.com/technomancy/leiningen) 1.1.0.

    $ lein deps
    $ lein compile-java
    $ lein compile
    $ lein test

Note that if you edit either `api.clj` or `testing.clj`, you should `lein compile` before running again.


## cascading-clojure is part of clj-sys http://github.com/clj-sys

- Conciseness, but not at the expense of expressiveness, explicitness, abstraction, and composability.

- Simple and explict functional sytle over macros and elaborate DSLs.

- Functional parameterization over vars and binding.

- Libraries over frameworks.

- Build in layers.

- Write tests.

- Copyright (c) Bradford Cross released under the MIT License (http://www.opensource.org/licenses/mit-license.php).