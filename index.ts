import * as Amqplib from 'amqplib-as-promised'

import { IPubSubParticipantOptions } from './lib/pub-sub-participant'
import { Publisher } from './lib/publisher'
import { Subscriber } from './lib/subscriber'

const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost'
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pings'
const CONSUMERS_NUMBER = Number(process.env.CONSUMERS_NUMBER) || 3
const SUBSCRIBER_EXPIRATION = Number(process.env.SUBSCRIBER_EXPIRATION) || 5

const options: IPubSubParticipantOptions = {
  amqpUrl: AMQP_URL,
  exchangeName: EXCHANGE_NAME
}

async function delay (seconds: number) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve()
    }, seconds * 1000)
  })
}

function getDummyMessageHandler (id: number | string) {
  return async (message: Amqplib.Message) => {
    console.log(`Subscriber (id: ${id}) received message: ` + message.content.toString())
    return true
  }
}

(async () => {
  async function initAllSubscribers () {
    return Promise.all(subscribers.map(async (subscriber) => {
      await subscriber.init()
      await subscriber.subscribe(getDummyMessageHandler(subscriber.getId()))
    }))
  }

  async function closeAllSubscribers () {
    return Promise.all(subscribers.map(async (subscriber) => {
      await subscriber.close()
    }))
  }

  const publisher = new Publisher(options)

  await publisher.init()

  await publisher.publish(Buffer.from('none should get it'))

  const subscribers: Subscriber[] = []

  for (let i = 1; i <= CONSUMERS_NUMBER - 1; i++) {
    subscribers.push(new Subscriber({
      ...options,
      subscriberId: `${i}`,
      expireAfter: SUBSCRIBER_EXPIRATION
    }))
  }

  await Promise.all(subscribers.map(async (subscriber: Subscriber) => {
    await subscriber.init()
    await subscriber.subscribe(getDummyMessageHandler(subscriber.getId()))
  }))

  await publisher.publish(Buffer.from('bum bum pow!'))

  const oneMoreSubscriber = new Subscriber({
    ...options,
    subscriberId: `${CONSUMERS_NUMBER}`,
    expireAfter: SUBSCRIBER_EXPIRATION
  })
  subscribers.push(oneMoreSubscriber)

  await oneMoreSubscriber.init()
  await oneMoreSubscriber.subscribe(getDummyMessageHandler(oneMoreSubscriber.getId()))

  await publisher.publish(Buffer.from('last call'))

  // close all
  await closeAllSubscribers()

  // reconnect all subscribers
  await initAllSubscribers()

  // close all subscribers
  await closeAllSubscribers()

  await publisher.publish(Buffer.from('just a silence, no one can hear me'))
  await delay(SUBSCRIBER_EXPIRATION * 2)

  // reconnect all subscribers
  await initAllSubscribers()

  // close all subscribers
  await closeAllSubscribers()

  // close publisher
  await publisher.close()
})()
