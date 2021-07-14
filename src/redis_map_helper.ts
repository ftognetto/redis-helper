import * as redis from 'redis';
import { promisify } from 'util';

export interface RedisMapHelperConfig<T> {
    client?: redis.RedisClient;
    host?: string;
    port?: number;
    prefix?: string;
    keyExtractor: (obj?: T) => string;
    ttl?: number;
}
export class RedisMapHelper<T> {

    _redis: redis.RedisClient;

    private _getAsync: (key: string) => Promise<string | null>;
    private _setAsync: (key: string, value: string) => Promise<unknown>;
    private _setExAsync: (key: string, ttl: number, value: string) => Promise<string>;
    private _delAsync: (key: string) => Promise<number>;

    private _lpushAsync: (key: string, value: string) => Promise<number>;
    private _rpushAsync: (key: string, value: string) => Promise<number>;
    private _lpopAsync: (key: string) => Promise<String>;
    private _rpopAsync: (key: string) => Promise<String>;
    
    private _lrangeAsync: (key: string, start: number, end: number) => Promise<string[]>;
    private _lindexAsync: (key: string, index: number) => Promise<string>;

    private _prefix: string;
    private _key: (obj?: T) => string;
    private _ttl: number;

    private _cacheDisabled = false;

    constructor(config: RedisMapHelperConfig<T>) {
        
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
        this._key = (obj?: T) => `${config.prefix}:${config.keyExtractor(obj)}`;
        this._ttl = config.ttl || 60 * 5;

        this._getAsync = promisify(this._redis.get).bind(this._redis);
        this._setAsync = promisify(this._redis.set).bind(this._redis);
        this._setExAsync = promisify(this._redis.setex).bind(this._redis);
        this._delAsync = promisify(this._redis.del).bind(this._redis);

        this._lpushAsync = promisify(this._redis.lpush).bind(this._redis);
        this._rpushAsync = promisify(this._redis.rpush).bind(this._redis);
        this._lpopAsync = promisify(this._redis.lpop).bind(this._redis);
        this._rpopAsync = promisify(this._redis.rpop).bind(this._redis);
        this._lrangeAsync = promisify(this._redis.lrange).bind(this._redis);
        this._lindexAsync = promisify(this._redis.lindex).bind(this._redis);
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
                if (this._ttl > 0) { await this._setExAsync(this._key(value), this._ttl, JSON.stringify(value)); }
                else { await this._setAsync(this._key(value), JSON.stringify(value)); }
            }
            catch (e) {
                // do nothing
            }
        }
    }

    async delCached(id: string): Promise<void> {
        await this._delAsync(id);
    }

    async pop(atTheEnd: boolean = false): Promise<void> {
        if (this._cacheDisabled) { return; }
        try {
            if (atTheEnd) {
                await this._rpopAsync(this._key());
            }
            else {
                await this._lpopAsync(this._key());
            }
        }
        catch (e) {
            throw e;
            // do nothing??
        }
    }

    async push(value: T, append: boolean = false): Promise<void> {
        if (this._cacheDisabled) { return; }
        try {
            if (append) {
                await this._rpushAsync(this._key(), JSON.stringify(value));
            }
            else {
                await this._lpushAsync(this._key(), JSON.stringify(value));
            }
            
        }
        catch (e) {
            throw e;
            // do nothing??
        }
    }

    async listRange(start: number, end: number): Promise<T[]> {
        if (this._cacheDisabled) { return []; }
        try {
            const values = await this._lrangeAsync(this._key(), start, end);
            return values.map((v) => JSON.parse(v));
        }
        catch (e) {
            return [];
        }
    }
    async listGet(index: number): Promise<T | null> {
        if (this._cacheDisabled) { return null; }
        try {
            const value = await this._lindexAsync(this._key(), index);
            return JSON.parse(value);
        }
        catch (e) {
            return null;
        }
    }

    async listClear(): Promise<void> {
        try {
            await this._delAsync(this._key());
        }
        catch (e) {
            // boh
        }
    }
}
