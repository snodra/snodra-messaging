# Hub protocol

The gRPC contract for the kraken hub lives in the umbrella repository at `protocol/hub.proto`
(`../kraken/protocol/hub.proto` in the documented clone layout).

`kraken-messaging/src/main/proto/hub.proto` is a verbatim copy of that file. The `com.google.protobuf`
Gradle plugin applied to the `kraken-messaging` module generates the Java messages, the grpc-java stubs
and the grpc-kotlin stubs (`HubGrpcKt`) from it during `./gradlew build`.

To re-sync after the umbrella proto changes, copy it over:

    cp ../kraken/protocol/hub.proto kraken-messaging/src/main/proto/hub.proto

Builder call: copy over submodule; the umbrella repo stays the source of truth.
