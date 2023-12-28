import { createClient, RedisClientType } from 'redis';

export interface RedisListHelperConfig<T> {
  client?: RedisClientType;
  url?: string;
  prefix?: string;
  keyExtractor: (obj: T) => string;
  ttl?: number;
}
export class RedisListHelper<T> {
  private _redis: RedisClientType;

  private _prefix: string;
  private _key: (obj: T) => string;
  private _ttl: number;

  private _cacheDisabled = false;

  constructor(config: RedisListHelperConfig<T>) {
    const _self = this;

    if (config.client) {
      this._redis = config.client;
    } else if (config.url) {
      this._redis = createClient({ url: config.url });
      this._redis.connect().catch((err) => {
        throw Error('[@quantos/redis-helper][RedisValueHelper] ' + err);
      });
    } else {
      throw Error('[@quantos/redis-helper][RedisValueHelper] invalid configuration. A client or host and port must be supplied.');
    }

    this._redis.on('error', function (error: any): void {
      console.error(error);
      if (error.code === 'ECONNREFUSED') {
        _self._cacheDisabled = true;
      }
      if (error.code === 'ETIMEDOUT') {
        _self._cacheDisabled = true;
        setTimeout(() => {
          _self._cacheDisabled = false;
        }, 10000);
      }
    });

    this._prefix = config.prefix || '';
    this._key = (obj) => `${config.prefix}:${config.keyExtractor(obj)}`;
    this._ttl = config.ttl || 60 * 5;
  }

  async getCacheds(ids: string[]): Promise<T[]> {
    const cacheds: T[] = [];
    if (this._cacheDisabled) {
      return cacheds;
    }
    try {
      for (const id of ids) {
        try {
          const data = await this._redis.get(`${this._prefix}:${id}`);
          if (data) {
            const cached: T = JSON.parse(data);
            cacheds.push(cached);
          }
        } catch (e) {
          // do nothing
        }
      }
    } catch (e) {
      console.log(e);
    }
    return cacheds;
  }

  async getCached(id: string): Promise<T | null> {
    const cacheds = await this.getCacheds([id]);
    if (cacheds && cacheds.length) {
      return cacheds[0];
    } else {
      return null;
    }
  }

  async setCached(values: T[]): Promise<void> {
    if (this._cacheDisabled) {
      return;
    }
    for (const value of values) {
      try {
        await this._redis.set(this._key(value), JSON.stringify(value), { EX: this._ttl });
      } catch (e) {
        // do nothing
      }
    }
  }

  async lPush(value: T): Promise<void> {
    if (this._cacheDisabled) {
      return;
    }
    try {
      await this._redis.lPush(this._key(value), JSON.stringify(value));
    } catch (e) {
      // do nothing??
    }
  }
}
