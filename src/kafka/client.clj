(ns kafka.client
  (:import [org.apache.kafka.clients.admin AdminClient NewTopic]
           [org.apache.kafka.clients.consumer KafkaConsumer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]))
(import [org.apache.kafka.clients.admin AdminClient])

(defn topics [admin-client]
  (.get (.names (.listTopics admin-client))))

(defn create-topic [admin-clint topic-name]
  (let [topic-name topic-name
        partitions 3
        replication-factor 1]
    (-> admin-clint
        (.createTopics [(NewTopic. topic-name partitions replication-factor)])
        .all
        .get)))

(defn send [producer topic k v]
  @(.send producer (ProducerRecord. k v)))

(comment
  (def admin-clint (AdminClient/create {"bootstrap.servers" "localhost:9092"}))
  (def producer
    (KafkaProducer.
     {"bootstrap.servers" "localhost:9092"
      "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
      "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"}))
  @(.send producer (ProducerRecord. "foo"
                                    "Some random key"
                                    "Clojure Rocks!"))

  (def consumer
    (KafkaConsumer.
     {"bootstrap.servers" "localhost:9092"
      "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
      "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
      "group.id" "my-group"}))
  (.subscribe consumer ["foo"])
  (->> (.poll consumer (java.time.Duration/ofMillis 200))
       (.iterator)
       (iterator-seq)
       (map (juxt #(.key %) #(.value %)))
       (into []))

  )
