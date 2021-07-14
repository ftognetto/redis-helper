import * as redis from 'redis';
import { promisify } from 'util';

export interface RedisListHelperConfig<T> {
    client?: redis.RedisClient;
    host?: string;
    port?: number;
    prefix?: string;
    keyExtractor: (obj: T) => string;
    ttl?: number;
}
export class RedisListHelper<T> {

    private _redis: redis.RedisClient;

    private _getAsync: (key: string) => Promise<string | null>;
    private _setAsync: (key: string, ttl: number, value: string) => Promise<string>;
    private _lpushAsync: (key: string, value: string) => Promise<number>;
    
    private _prefix: string;
    private _key: (obj: T) => string;
    private _ttl: number;

    private _cacheDisabled = false;

    constructor(config: RedisListHelperConfig<T>) {
        
        if (config.client) { this._redis = config.client; }
        else if (config.port && config.host) { this._redis = redis.createClient(config.port, config.host, { enable_offline_queue: false }); }
        else { throw Error('[@quantos/redis-helper][RedisValueHelper] invalid configuration. A client or host and port must be supplied.') }
        
        const _self = this;
        this._redis.on('error', function(error: any): void {
            console.error(error);
            if (error.code === 'ECONNREFUSED') {
                _self._cacheDisabled = true;
            }
            if (error.code === 'ETIMEDOUT') {
                _self._cacheDisabled = true;
                setTimeout(() => { _self._cacheDisabled = false; }, 10000);
            }
            
        });

        this._prefix = config.prefix || '';
        this._key = (obj) => `${config.prefix}:${config.keyExtractor(obj)}`;
        this._ttl = config.ttl || 60 * 5;

        this._getAsync = promisify(this._redis.get).bind(this._redis);
        this._setAsync = promisify(this._redis.setex).bind(this._redis);
        this._lpushAsync = promisify(this._redis.lpush).bind(this._redis);

    }

    async getCacheds(ids: string[]): Promise<T[]> {
        const cacheds: T[] = [];
        if (this._cacheDisabled) { return cacheds; }
        try {
            for (const id of ids) {
                try { 
                    const data = await this._getAsync(`${this._prefix}:${id}`);
                    if (data) {
                        const cached: T = JSON.parse(data);
                        cacheds.push(cached);
                    }
                }
                catch (e) {
                    // do nothing
                }
            }
        }
        catch (e) {
            console.log(e);
        }
        return cacheds;
    }

    async getCached(id: string): Promise<T | null> {
        const cacheds = await this.getCacheds([id]);
        if (cacheds && cacheds.length) { return cacheds[0]; }
        else { return null; }
    }

    async setCached(values: T[]): Promise<void> {
        if (this._cacheDisabled) { return; }
        for (const value of values) {
            try {
                await this._setAsync(this._key(value), this._ttl, JSON.stringify(value));
            }
            catch (e) {
                // do nothing
            }
        }
    }

    async lPush(value: T): Promise<void> {
        if (this._cacheDisabled) { return; }
        try {
            await this._lpushAsync(this._key(value), JSON.stringify(value));
        }
        catch (e) {
            // do nothing??
        }
    }

}
