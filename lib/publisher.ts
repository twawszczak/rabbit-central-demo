import { PubsSbParticipant } from './pub-sub-participant'

export class Publisher extends PubsSbParticipant {
  public async init () {
    await super.init()
    await this.channel.assertExchange(this.options.exchangeName, 'fanout')
  }

  public async publish (message: Buffer) {
    await this.channel.publishWithConfirmation(this.options.exchangeName, '', message)
  }

  public async close () {
    await this.channel.deleteExchange(this.options.exchangeName)
    return super.close()
  }
}
