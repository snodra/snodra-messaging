# Hub protocol

The gRPC contract for the kraken hub lives in the umbrella repository at `protocol/hub.proto`
(`../kraken/protocol/hub.proto` in the documented clone layout).

Nothing is compiled from it yet. Before implementing the hub client in `kraken-messaging`:

1. Add the umbrella repo as a git submodule here (`git submodule add <kraken-umbrella-url> proto/kraken`)
   or copy `hub.proto` into `proto/`.
2. Apply the `com.google.protobuf` Gradle plugin (version in `gradle/libs.versions.toml`) to the
   `kraken-messaging` module with the `grpc` and `grpckt` plugins, pointing `sourceSets.main.proto`
   at this directory.

Builder call: submodule over copy, so the proto has exactly one source of truth.
