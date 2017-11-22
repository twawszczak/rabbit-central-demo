import * as Amqplib from 'amqplib-as-promised'

import { IPubSubParticipantOptions, PubsSbParticipant } from './pub-sub-participant'

export interface ISubscriberOptions extends IPubSubParticipantOptions {
  subscriberId: string
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
    await this.channel.assertQueue(this.queueName, { autoDelete: true })
    await this.channel.bindQueue(this.queueName, this.options.exchangeName, '')
  }

  public async subscribe (handler: (message: Amqplib.Message | null) => any) {
    this.consumerTag = (await this.channel.consume(this.queueName, handler)).consumerTag
  }

  public getId () {
    return this.options.subscriberId
  }

  public async close () {
    return super.close()
  }
}
