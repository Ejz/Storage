<?php

namespace Ejz;

use RuntimeException;
// use Generator;
// use Error;
// use Amp\Loop;
// use Amp\Promise;
// use Amp\Deferred;
// use Amp\Producer;

class Storage
{
    protected const INVALID_REPOSITORY_ERROR = 'INVALID_REPOSITORY_ERROR: %s';

    /** @var DatabasePool */
    protected $pool;

    /** @var RedisCache */
    protected $cache;

    /** @var Bitmap */
    protected $bitmap;

    /** @var array */
    protected $repositories;

    /** @var array */
    protected $cached;

    /**
     * @param DatabasePool $pool
     * @param RedisCache   $cache
     * @param Bitmap       $bitmap
     * @param array        $repositories
     */
    public function __construct(
        DatabasePool $pool,
        RedisCache $cache,
        Bitmap $bitmap,
        array $repositories
    ) {
        $this->pool = $pool;
        $this->cache = $cache;
        $this->bitmap = $bitmap;
        $this->repositories = $repositories;
        $this->cached = [];
    }

    /**
     * @return DatabasePool
     */
    public function getPool(): DatabasePool
    {
        return $this->pool;
    }

    /**
     * @return RedisCache
     */
    public function getCache(): RedisCache
    {
        return $this->cache;
    }

    /**
     * @return Bitmap
     */
    public function getBitmap(): Bitmap
    {
        return $this->bitmap;
    }

    /**
     * @return array
     */
    public function getRepositories(): array
    {
        return $this->repositories;
    }

    /**
     * @param string $table
     * @param array  $arguments
     *
     * @return Repository
     */
    public function __call(string $name, array $arguments): Repository
    {
        $cached = $this->cached[$name] ?? null;
        if ($cached !== null) {
            return $cached;
        }
        $config = $this->repositories[$name] ?? null;
        if ($config === null) {
            throw new RuntimeException(sprintf(self::INVALID_REPOSITORY_ERROR, $name));
        }
        $this->cached[$name] = new Repository($this, $name, $config);
        return $this->cached[$name];
    }

    /**
     * @param int $primary
     *
     * @return array
     */
    public static function getPrimarySecondaryClusterConfig(int $primary): array
    {
        $isPrimaryTable = function ($name, $names) use ($primary) {
            return array_search($name, $names) === $primary;
        };
        return compact('isPrimaryTable');
    }

    /**
     * @param ?string $field
     *
     * @return array
     */
    public static function getShardsClusterConfig(?string $field = null): array
    {
        $getPoolClosure = function ($readable) use ($field) {
            return function ($id, $values, $names, $nargs) use ($readable, $field) {
                $c = count($names);
                $keys = array_keys($names);
                if ($id !== null) {
                    $id = abs($id);
                    $id %= $c;
                    $id -= 1;
                    $id = $id < 0 ? $id + $c : $id;
                    $key = $keys[$id];
                    return [$names[$key]];
                }
                $values = $values ?? [];
                $v = $field === null ? null : ($values[$field] ?? null);
                if ($v !== null) {
                    $crc = crc32($v);
                    $crc %= $c;
                    $key = $keys[$crc];
                    return [$names[$key]];
                }
                if ($readable) {
                    return $nargs === 0 ? $names : [];
                }
                return [$names[array_rand($names)]];
            };
        };
        $getReadablePool = $getPoolClosure(true);
        $getWritablePool = $getPoolClosure(false);
        $getPkStartWith = function ($name, $names) {
            $_ = (int) array_search($name, $names);
            return $_ + 1;
        };
        $getPkIncrementBy = function ($name, $names) {
            return count($names);
        };
        return compact([
            'getReadablePool',
            'getWritablePool',
            'getPkStartWith',
            'getPkIncrementBy',
        ]);
    }
}
