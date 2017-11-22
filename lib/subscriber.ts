import * as Amqplib from 'amqplib-as-promised'

import { IPubSubParticipantOptions, PubsSbParticipant } from './pub-sub-participant'

export interface ISubscriberOptions extends IPubSubParticipantOptions {
  subscriberId: string
  expireAfter?: number
}

export class Subscriber extends PubsSbParticipant {
  protected readonly queueName: string
  protected consumerTag?: string

  constructor (protected options: ISubscriberOptions) {
    super(options)
    this.queueName = `${options.subscriberId}-subscribe`
  }

  public async init () {
    await super.init()
    await this.channel.assertQueue(this.queueName, { expires: (this.options.expireAfter || 10) * 1000 /* 10 s. */ })
    await this.channel.bindQueue(this.queueName, this.options.exchangeName, '')
  }

  public async subscribe (handler: (message: Amqplib.Message) => Promise<boolean>) {
    this.consumerTag = (await this.channel.consume(this.queueName, async (message: Amqplib.Message | null) => {
      if (message) {
        const ack: boolean = await handler(message)
        if (ack) {
          this.channel.ack(message)
        }
      }

    })).consumerTag
  }

  public getId () {
    return this.options.subscriberId
  }

  public async close () {
    return super.close()
  }
}
