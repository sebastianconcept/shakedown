//! Compile-pass test: #[shakedown::test] with Kafka resource (feature-gated).
//!
//! Expected to compile and pass when `kafka` feature is enabled.

#[cfg(feature = "kafka")]
#[cfg(test)]
mod tests {
    use shakedown::{Context, resources::KafkaResource};

    // Test with Kafka resource specified in attribute (feature-gated)
    #[shakedown::test(resources = [Kafka])]
    async fn with_kafka_resource(ctx: &mut Context) {
        let _bindings = ctx.kafka_bindings();
    }
}
