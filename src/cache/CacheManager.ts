export class CacheManager {
    private cache = new Map<string, any>();

    get(key: string) {
        return this.cache.get(key);
    }

    set(key: string, value: any) {
        this.cache.set(key, value);
    }

    clear() {
        this.cache.clear();
    }
}