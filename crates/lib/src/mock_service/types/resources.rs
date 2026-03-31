//! Shared infrastructure handles available to mock service implementations while handling a request.

use std::fmt;

#[cfg(feature = "kafka")]
use rdkafka::producer::FutureProducer;

#[cfg(feature = "rabbitmq")]
use lapin::Channel;

#[cfg(feature = "kafka")]
use crate::resources::KafkaBindings;

#[cfg(feature = "mariadb")]
use crate::resources::MariaDBBindings;
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
use crate::resources::PostgresBindings;
#[cfg(feature = "rabbitmq")]
use crate::resources::RabbitBindings;

/// Shared infrastructure handles available to mock service implementations while handling a request.
///
/// Populated by the harness after resource provision; use [`MockServiceResources::default`] for unit tests
/// when no infrastructure is wired.
#[derive(Default)]
pub struct MockServiceResources {
    #[cfg(feature = "kafka")]
    pub kafka_bindings: Option<KafkaBindings>,
    #[cfg(feature = "kafka")]
    pub kafka_producer: Option<FutureProducer>,
    #[cfg(feature = "rabbitmq")]
    pub rabbit_bindings: Option<RabbitBindings>,
    #[cfg(feature = "rabbitmq")]
    pub rabbit_channel: Option<Channel>,
    #[cfg(feature = "postgres")]
    pub postgres_bindings: Option<PostgresBindings>,
    #[cfg(feature = "timescaledb")]
    pub timescaledb_bindings: Option<PostgresBindings>,
    #[cfg(feature = "mariadb")]
    pub mariadb_bindings: Option<MariaDBBindings>,
}

impl fmt::Debug for MockServiceResources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("MockServiceResources");
        #[cfg(feature = "kafka")]
        {
            s.field("kafka_bindings", &self.kafka_bindings);
            s.field(
                "kafka_producer",
                &self.kafka_producer.as_ref().map(|_| "<FutureProducer>"),
            );
        }
        #[cfg(feature = "rabbitmq")]
        {
            s.field("rabbit_bindings", &self.rabbit_bindings);
            s.field(
                "rabbit_channel",
                &self.rabbit_channel.as_ref().map(|_| "<lapin::Channel>"),
            );
        }
        #[cfg(feature = "postgres")]
        {
            s.field("postgres_bindings", &self.postgres_bindings);
        }
        #[cfg(feature = "timescaledb")]
        {
            s.field("timescaledb_bindings", &self.timescaledb_bindings);
        }
        #[cfg(feature = "mariadb")]
        {
            s.field("mariadb_bindings", &self.mariadb_bindings);
        }
        s.finish()
    }
}
