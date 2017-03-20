# RabbitMQ client for permanent publisher/consumer connections

Configuring a client
```python
url = 'amqp://user:password@amqphost/exchangename'
exchange = 'exchangename'
extype = 'topic'    # example
queue = 'queuename'
topic = 'some.topic'

# Act as producer
bunnyclient = BunnyChat(url, exchange, extype, input_queue=consumer_input)

# Act as consumer with no queue to pass messages out
bunnyclient = BunnyChat(url, exchange, extype, queue, topic)

# Act as consumer and producer with input queue. Will start a dequer thread.
bunnyclient = BunnyChat(url, exchange, extype, queue, topic, input_queue=consumer_input)

# Act as producer with no input queue - will not create a thread to dequeue commands
bunnyclient = BunnyChat(url, exchange, extype)
```

## Running the client
```python
# Run the client
bunnyclient.run()
```
