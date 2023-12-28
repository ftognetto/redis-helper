import { createClient, RedisClientType } from 'redis';

export interface RedisMapHelperConfig<T> {
  client?: RedisClientType;
  url?: string;
  prefix?: string;
  keyExtractor: (obj?: T) => string;
  ttl?: number;
}
export class RedisMapHelper<T> {
  _redis: RedisClientType;

  private _prefix: string;
  private _key: (obj?: T) => string;
  private _ttl: number;

  private _cacheDisabled = false;

  constructor(config: RedisMapHelperConfig<T>) {
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

    const _self = this;
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
    this._key = (obj?: T) => `${config.prefix}:${config.keyExtractor(obj)}`;
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
        if (this._ttl > 0) {
          await this._setExAsync(this._key(value), this._ttl, JSON.stringify(value));
        } else {
          await this._setAsync(this._key(value), JSON.stringify(value));
        }
      } catch (e) {
        // do nothing
      }
    }
  }

  async delCached(id: string): Promise<void> {
    await this._delAsync(id);
  }

  async pop(atTheEnd: boolean = false): Promise<void> {
    if (this._cacheDisabled) {
      return;
    }
    try {
      if (atTheEnd) {
        await this._rpopAsync(this._key());
      } else {
        await this._lpopAsync(this._key());
      }
    } catch (e) {
      throw e;
      // do nothing??
    }
  }

  async push(value: T, append: boolean = false): Promise<void> {
    if (this._cacheDisabled) {
      return;
    }
    try {
      if (append) {
        await this._rpushAsync(this._key(), JSON.stringify(value));
      } else {
        await this._lpushAsync(this._key(), JSON.stringify(value));
      }
    } catch (e) {
      throw e;
      // do nothing??
    }
  }

  async listRange(start: number, end: number): Promise<T[]> {
    if (this._cacheDisabled) {
      return [];
    }
    try {
      const values = await this._lrangeAsync(this._key(), start, end);
      return values.map((v) => JSON.parse(v));
    } catch (e) {
      return [];
    }
  }
  async listGet(index: number): Promise<T | null> {
    if (this._cacheDisabled) {
      return null;
    }
    try {
      const value = await this._lindexAsync(this._key(), index);
      return JSON.parse(value);
    } catch (e) {
      return null;
    }
  }

  async listClear(): Promise<void> {
    try {
      await this._delAsync(this._key());
    } catch (e) {
      // boh
    }
  }
}
