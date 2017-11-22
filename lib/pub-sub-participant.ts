import * as Amqplib from 'amqplib-as-promised'

export interface IPubSubParticipantOptions {
  amqpUrl: string
  exchangeName: string
}

export class PubsSbParticipant {
  protected connection: Amqplib.Connection
  protected channel: Amqplib.Channel

  constructor (protected options: IPubSubParticipantOptions) {}

  public async init (): Promise<void> {
    this.connection = await Amqplib.connect(this.options.amqpUrl)
    this.channel = await this.connection.createChannel()
  }

  public async close (): Promise<any> {
    await this.channel.close()
    void this.connection.close()

    return this.connection.waitForClose()
  }
}
