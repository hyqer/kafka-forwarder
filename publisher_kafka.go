package main

import (
	"github.com/Shopify/sarama"

	"math/rand"

	"time"
)

// Support for newer SSL signature algorithms
//import _ "crypto/sha256"
//import _ "crypto/sha512"

//var hostname string
//var hostport_re, _ = regexp.Compile("^(.+):([0-9]+)$")

func init() {
	//hostname, _ = os.Hostname()
	rand.Seed(time.Now().UnixNano())
}

func PublishKafka(input chan []*FileEvent,
	registrar chan []*FileEvent,
	config *NetworkConfig) {
	var err error
	var sequence uint32
	clientConfig := sarama.NewClientConfig()
	client, err := sarama.NewClient("logcollector", config.Servers, clientConfig)
	if err != nil {
		emit("failed to create kafka client", err)
		return
	}

	producerConfig := sarama.NewProducerConfig()
	producerConfig.Partitioner = sarama.NewRandomPartitioner()
	producerConfig.MaxBufferedBytes = uint32(1024 * 1024)
	producerConfig.MaxBufferTime = time.Duration(1000) * time.Millisecond
	producer, err := sarama.NewProducer(client, producerConfig)

	if err != nil {
		emit("failed to create kafka producer", err)
		return
	}
	for events := range input {

		for _, event := range events {
			sequence += 1
			//send here
			oops := func(err error) {
				// TODO(sissel): Track how frequently we timeout and reconnect. If we're
				// timing out too frequently, there's really no point in timing out since
				// basically everything is slow or down. We'll want to ratchet up the
				// timeout value slowly until things improve, then ratchet it down once
				// things seem healthy.
				emit("Socket error, will reconnect: %s\n", err)
				time.Sleep(1 * time.Second)
			}
			for {
				//_, err = producer.Publish(kafka.NewCompressedMessage([]byte(*event.Text)))
				err = producer.SendMessage(config.Topic, nil, sarama.StringEncoder(*event.Text))
				//println("topic:" + config.Topic)
				//println("conyent:" + *event.Text)
				if err != nil {
					oops(err)
					continue
				}
				break
			}
		}

		// Tell the registrar that we've successfully sent these events
		registrar <- events
	}
}
