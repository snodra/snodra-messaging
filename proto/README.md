# Server protocol

The gRPC contract for the Snodra server lives in the public `snodra-server` repository at
`proto/hub.proto` (`../snodra-server/proto/hub.proto` in the documented clone layout). That file is
the source of truth.

`snodra-messaging/src/main/proto/hub.proto` is a verbatim copy of it. The `com.google.protobuf`
Gradle plugin applied to the `snodra-messaging` module generates the Java messages, the grpc-java
stubs and the grpc-kotlin stubs (`HubGrpcKt`) from it during `./gradlew build`; the generated Java
package is `com.damoreira.snodra.server.v1`, as the proto declares.

To re-sync after the protocol changes, copy it over:

    cp ../snodra-server/proto/hub.proto snodra-messaging/src/main/proto/hub.proto

`scripts/check-proto-sync.sh` (and the `checkProtoSync` Gradle task) compares the SHA-256 of the two
files; CI's `proto-sync` job clones `https://gitlab.damoreira.ml/snodra/snodra-server.git` and runs
it, so a drift fails the pipeline.
