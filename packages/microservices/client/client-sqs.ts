import { ClientProxy } from "./client-proxy";
import { SQSOptions, ReadPacket, WritePacket, SQSSendMessageParams, SQSReceiveMessageParams } from "./../interfaces";
import { Logger } from "@nestjs/common";
import { EventEmitter } from "events";
import { loadPackage } from "@nestjs/common/utils/load-package.util";
import { SQS } from "aws-sdk";
import { randomStringGenerator } from "@nestjs/common/utils/random-string-generator.util";

const REPLY_QUEUE = 'amq.sqs.reply-to';

export class ClientSQS extends ClientProxy {

  protected readonly logger = new Logger(ClientProxy.name);
  protected connection: Promise<any>;
  protected responseEmitter: EventEmitter;
  protected params: SQSSendMessageParams;
  protected sqs: SQS = null;
  protected sqsOptions: SQS.Types.ClientConfiguration;
  protected receiveParams: SQSReceiveMessageParams;
  protected sendQueue: string;
  protected interval: NodeJS.Timeout;

  constructor(protected readonly options: SQSOptions['options']) {
    super();

    this.params = this.getOptionsProp(this.options, 'params');
    this.sqsOptions = this.getOptionsProp(this.options, 'client');
    this.receiveParams = { QueueUrl: REPLY_QUEUE };
    Object.assign(this.receiveParams, this.getOptionsProp(this.options, 'receiveParams') || {});

    loadPackage('aws-sdk', ClientSQS.name, () => require('aws-sdk')); 
    this.sqs = new SQS(this.sqsOptions);
  }

  async connect(): Promise<any> {
    const sendQueue = await this.getQueueUrl('amq.sqs.send');
    const replyTo = await this.getQueueUrl(REPLY_QUEUE);

    this.sendQueue = sendQueue;
    this.interval = setInterval(async () => await this.pollMessages(replyTo), 500);
  }

  close(): any {
    clearInterval(this.interval);
  }

  async getQueueUrl(queue: string): Promise<string> {
    let getQueue: string;
    
    await this.sqs.getQueueUrl({
      QueueName: queue,
    }, (err, data) => {
      if (err) {
        throw err;
      }

      getQueue = data.QueueUrl;
    });

    if (!getQueue) {
      let queueUrl: string;

      await this.sqs.createQueue({
        QueueName: queue,
      }, (err, data) => {
        if (err) {
          throw err;
        }

        queueUrl = data.QueueUrl;
      });

      return queueUrl;
    } else return getQueue;
  }

  sendMessage(message: string, queueUrl: string): void {
    this.sqs.sendMessage({
      QueueUrl: queueUrl,
      MessageBody: message,
    });
  }

  pollMessages(queueUrl: string): void {
    this.sqs.receiveMessage({
      QueueUrl: queueUrl,
    }, (err, data) => {
      if (err) {
        throw err;
      }

      data.Messages.forEach((message: SQS.Types.Message) => {

        try {
          const body = JSON.parse(message.Body);

          if (!body.correlationId) {
            return;
          }

          this.responseEmitter.emit(body.correlationId, message);
          this.deleteMessage(queueUrl, message.ReceiptHandle);
        } catch (e) {
          // Not sure if to log?
          // Could be message without correlationId 
        }
      });
    });
  }

  deleteMessage(queueUrl: string, receipt: string): void {
    this.sqs.deleteMessage({
      QueueUrl: queueUrl,
      ReceiptHandle: receipt,
    });
  }

  public handleMessage(
    packet: WritePacket,
    callback: (packet: WritePacket) => any,
  ) {
    const { err, response, isDisposed } = packet;
    if (isDisposed || err) {
      callback({
        err,
        response: null,
        isDisposed: true,
      });
    }
    callback({
      err,
      response,
    });
  }

  protected publish(
    message: ReadPacket,
    callback: (packet: WritePacket) => void,
  ): Function {
    try {
      const correlationId = randomStringGenerator();
      const listener = ({ content }: { content: any }) =>
        this.handleMessage(JSON.parse(content.toString()), callback);

      Object.assign(message, { id: correlationId });
      this.responseEmitter.on(correlationId, listener);
      this.sendMessage(
        this.sendQueue,
        JSON.stringify(message),
      );
      return () => this.responseEmitter.removeListener(correlationId, listener);
    } catch (err) {
      callback({ err });
    }
  }

  public dispatchEvent<T = any>(packet: ReadPacket): Promise<T> {
    return new Promise((resolve, reject) => {
      this.sqs.sendMessage({
        QueueUrl: this.sendQueue,
        MessageBody: JSON.stringify(packet),
      }, (err, data) => {
        if (err) {
          reject(err);
        }
        else resolve();
      });
    });
  }
}
