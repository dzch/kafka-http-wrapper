package main

import (
    "github.com/Shopify/sarama"
    "fmt"
    "flag"
    "os"
)

var (
    brokerIp = flag.String("ip", "127.0.0.1", "ip of one broker")
    brokerPort = flag.Int("port", 0, "port of one broker")
    topic = flag.String("topic", "not-exist-topic", "topic to be read")
    partition = flag.Int("partition", 0, "partition to be read")
    offset = flag.Int64("offset", 1, "offset to be read")
)


func main() {
    flag.Parse()

    consumer, err := sarama.NewConsumer([]string{fmt.Sprintf("%s:%d", *brokerIp, *brokerPort)}, nil)
    if err != nil {
        fmt.Println(err)
        os.Exit(255)
    }
    defer func() {
        if err := consumer.Close(); err != nil {
            fmt.Println(err)
            os.Exit(255)
        }
    }()

    partitionConsumer, err := consumer.ConsumePartition(*topic, int32(*partition), *offset)
    if err != nil {
        fmt.Println(err)
        os.Exit(255)
    }
    defer func() {
        if err := partitionConsumer.Close(); err != nil {
            fmt.Println(err)
            os.Exit(255)
        }
    }()

    msg := <-partitionConsumer.Messages()
    vlen, err := os.Stdout.Write(msg.Value)
    os.Stderr.WriteString(fmt.Sprintf("\ndata len: %d\n", vlen))
    if err != nil {
        os.Stderr.WriteString(fmt.Sprintln("error: ", err))
        os.Exit(255)
    }
    os.Exit(0)
}

//// Trap SIGINT to trigger a shutdown.
//signals := make(chan os.Signal, 1)
//signal.Notify(signals, os.Interrupt)
//
//consumed := 0
//ConsumerLoop:
//for {
//    select {
//    case msg := <-partitionConsumer.Messages():
//        log.Printf("Consumed message offset %d\n", msg.Offset)
//        consumed++
//    case <-signals:
//        break ConsumerLoop
//    }
//}
//
//log.Printf("Consumed: %d\n", consumed)
