import * as redis from 'redis';
import { promisify } from 'util';

export interface RedisValueHelperConfig<T> {
    client?: redis.RedisClient;
    host?: string;
    port?: number;
    prefix?: string;
    ttl?: number;
    disableListeners?: boolean;
}
export class RedisValueHelper<T> {

    private _redis: redis.RedisClient;

    private _getAsync: (key: string) => Promise<string | null>;
    private _setAsync: (key: string, ttl: number, value: string) => Promise<string>;
    
    private _prefix: string;
    private _ttl: number;

    private _cacheDisabled = false;

    constructor(config: RedisValueHelperConfig<T>) {

        if (config.client) { this._redis = config.client; }
        else if (config.port && config.host) { this._redis = redis.createClient(config.port, config.host, { enable_offline_queue: false }); }
        else { throw Error('[@quantos/redis-helper][RedisValueHelper] invalid configuration. A client or host and port must be supplied.') }

        const _self = this;
        if (!config.disableListeners) {
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
        }

        this._prefix = config.prefix || '';
        this._ttl = config.ttl || 60 * 5;

        this._getAsync = promisify(this._redis.get).bind(this._redis);
        this._setAsync = promisify(this._redis.setex).bind(this._redis);

    }

    async getCached(key: string): Promise<T | null> {
        if (this._cacheDisabled) { return null; }
      
        try { 
            const data = await this._getAsync(`${this._prefix}:${key}`);
            if (data) {
                const cached: T = JSON.parse(data);
                return cached;
            }
        }
        catch (e) {
            // do nothing
        }
       
        return null;
    }

    async getCacheds(keys: string[]): Promise<T[]> {
        const cacheds: T[] = [];
        for (const key of keys) {
            const cached = await this.getCached(key);
            if (cached) { cacheds.push(cached); }
        }
        return cacheds;
    }

    async setCached(key: string, value: T): Promise<void> {
        if (this._cacheDisabled) { return; }
        try {
            await this._setAsync(key, this._ttl, JSON.stringify(value));
        }
        catch (e) {
            // do nothing
        }
    }

}
