import * as Amqplib from 'amqplib-as-promised'

import { IPubSubParticipantOptions } from './lib/pub-sub-participant'
import { Publisher } from './lib/publisher'
import { Subscriber } from './lib/subscriber'

const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost'
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pings'
const CONSUMERS_NUMBER = Number(process.env.CONSUMERS_NUMBER) || 3

const options: IPubSubParticipantOptions = {
  amqpUrl: AMQP_URL,
  exchangeName: EXCHANGE_NAME
}

function getDummyMessageHandler (id: number | string) {
  return (message: Amqplib.Message | null) => {
    if (message) {
      console.log(`Subscriber (id: ${id}) received message: ` + message.content.toString())
    }
  }
}

(async () => {
  const publisher = new Publisher(options)

  await publisher.init()

  await publisher.publish(Buffer.from('none should get it'))

  const subscribers: Subscriber[] = []

  for (let i = 1; i <= CONSUMERS_NUMBER - 1; i++) {
    subscribers.push(new Subscriber({
      ...options,
      subscriberId: `${i}`
    }))
  }

  await Promise.all(subscribers.map(async (subscriber: Subscriber) => {
    await subscriber.init()
    await subscriber.subscribe(getDummyMessageHandler(subscriber.getId()))
  }))

  await publisher.publish(Buffer.from('bum bum pow!'))

  const oneMoreSubscriber = new Subscriber({
    ...options,
    subscriberId: `${CONSUMERS_NUMBER}`
  })
  subscribers.push(oneMoreSubscriber)

  await oneMoreSubscriber.init()
  await oneMoreSubscriber.subscribe(getDummyMessageHandler(oneMoreSubscriber.getId()))

  await publisher.publish(Buffer.from('last call'))

  await new Promise((resolve) => {
    setTimeout(() => {
      resolve()
    }, 1000)
  })

  await Promise.all(subscribers.map(async (subscriber) => {
    await subscriber.close()
  }))

  await publisher.close()
})()
