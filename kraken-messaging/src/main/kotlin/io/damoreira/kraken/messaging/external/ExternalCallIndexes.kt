package io.damoreira.kraken.messaging.external

import org.springframework.beans.factory.InitializingBean
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.index.Index
import java.time.Duration

/** Ensures the TTL index of `external_calls` (on `updatedAt`, after [retention]) exists. Idempotent. */
class ExternalCallIndexes(
    private val mongo: MongoTemplate,
    private val retention: Duration,
) : InitializingBean {
    override fun afterPropertiesSet() {
        mongo.indexOps("external_calls").createIndex(
            Index().on("updatedAt", Sort.Direction.ASC).expire(retention).named("updatedAt_ttl"),
        )
    }
}
