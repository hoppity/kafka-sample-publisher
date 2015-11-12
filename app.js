var kafka = require('kafka-node'),
    Promise = require('promise'),
    config = require('config-node')(),
    util = require('util'),

    clientId = config.clientId,

    client = new kafka.Client(config.kafka.zkConnect, clientId),
    producer = new kafka.HighLevelProducer(client, { requireAcks: 1, ackTimeoutMs: 100 }),

    producerInterval,

    exit = function () {
        clearInterval(producerInterval);
        producer.close(function () {
            process.exit();
        });
    },

    stocks = [{
        name: 'AXJO',
        price: 5122.60
    }, {
        name: 'AORD',
        price: 5181.10
    }, {
        name: 'AAPL',
        price: 116.12
    }, {
        name: 'GOOG',
        price: 735.40
    }, {
        name: 'YHOO',
        price: 33.38
    }];

producer.on('ready', function () {
    producer.createTopics([config.kafka.topic], false, function () {});

    producerInterval = setInterval(function () {

        var index = Math.floor(Math.random() * stocks.length),
            stock = stocks[index],
            delta = Math.floor(Math.random() * stock.price * 10) / 100,
            multiplier = Math.random() < 0.5 ? -1 : 1,
            message = { _timestamp: Date.now(), item: stock };

        stock.price = Math.floor((stock.price + multiplier * delta) * 100) / 100;
        
        producer.send(
            [{
                topic: config.kafka.topic,
                messages: new kafka.KeyedMessage(stock.name, JSON.stringify(message)),
                attributes: 2
            }], function (e) {
                if (e) {
                    console.error("Error sending message to destination: " + e);
                }
            });

    }, 100);
});

process.on('SIGINT', function () {
    console.log('Shutting down...');
    exit();
});

