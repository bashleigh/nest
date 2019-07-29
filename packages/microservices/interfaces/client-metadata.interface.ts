import { Transport } from './../enums/transport.enum';
import {
  GrpcOptions,
  MqttOptions,
  NatsOptions,
  RedisOptions,
  RmqOptions,
  SQSOptions,
} from './microservice-configuration.interface';

export type ClientOptions =
  | RedisOptions
  | NatsOptions
  | MqttOptions
  | GrpcOptions
  | TcpClientOptions
  | RmqOptions
  | SQSOptions;

export interface TcpClientOptions {
  transport: Transport.TCP;
  options?: {
    host?: string;
    port?: number;
  };
}
