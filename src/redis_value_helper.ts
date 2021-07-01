import * as redis from 'redis';
import { promisify } from 'util';

export interface RedisValueHelperConfig<T> {
    host: string;
    port: number;
    prefix?: string;
    ttl?: number;
}
export class RedisValueHelper<T> {

    private _redis: redis.RedisClient;

    private _getAsync: (key: string) => Promise<string | null>;
    private _setAsync: (key: string, ttl: number, value: string) => Promise<string>;
    
    private _prefix: string;
    private _ttl: number;

    private _cacheDisabled = false;

    constructor(config: RedisValueHelperConfig<T>) {
        this._redis = redis.createClient(config.port, config.host);
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
