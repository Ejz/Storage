<?php

namespace Ejz;

use RuntimeException;
use Amp\Promise;

class Storage
{
    protected const INVALID_REPOSITORY_ERROR = 'INVALID_REPOSITORY_ERROR: %s';

    /** @var Pool */
    protected $databasePool;

    /** @var Pool */
    protected $bitmapPool;

    /** @var RedisCache */
    protected $cache;

    /** @var array */
    protected $repositories;
    
    /** @var array */
    protected $cached;

    /**
     * @param Pool       $databasePool
     * @param Pool       $bitmapPool
     * @param RedisCache $cache
     * @param array      $repositories
     */
    public function __construct(
        Pool $databasePool,
        Pool $bitmapPool,
        RedisCache $cache,
        array $repositories
    ) {
        $this->databasePool = $databasePool;
        $this->bitmapPool = $bitmapPool;
        $this->cache = $cache;
        $this->repositories = $repositories;
        $this->cached = [];
    }

    /**
     * @return DatabasePool
     */
    public function getDatabasePool(): Pool
    {
        return $this->databasePool;
    }

    /**
     * @return DatabasePool
     */
    public function getBitmapPool(): Pool
    {
        return $this->bitmapPool;
    }

    /**
     * @return RedisCache
     */
    public function getCache(): RedisCache
    {
        return $this->cache;
    }

    /**
     * @return array
     */
    public function getRepositories(): array
    {
        return $this->repositories;
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        $cached = $this->cached[$name] ?? null;
        if ($cached !== null) {
            return $cached;
        }
        if (substr($name, -4) === 'Sync') {
            $name = substr($name, 0, -4);
            return Promise\wait($this->$name(...$arguments));
        }
        if (in_array($name, ['create', 'drop'])) {
            $promises = [];
            foreach (array_keys($this->repositories) as $_) {
                $promises[$_] = $this->$_()->$name();
            }
            return Promise\all($promises);
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
        $primaryPoolFilter = function ($smth, $names) use ($primary) {
            return [$names[$primary]];
        };
        $readablePoolFilter = function ($smth, $names) use ($primary) {
            return [$names[$primary]];
        };
        $writablePoolFilter = function ($smth, $name) {
            return $names;
        };
        return compact('primaryPoolFilter', 'readablePoolFilter', 'writablePoolFilter');
    }

    /**
     * @param ?string $field     (optional)
     * @param bool    $fieldAsId (optional)
     *
     * @return array
     */
    public static function getShardsClusterConfig(
        ?string $field = null,
        bool $fieldAsId = false
    ): array {
        $getPoolClosure = function ($readable) use ($field, $fieldAsId) {
            return function ($id, $values, $names, $nargs) use ($readable, $field, $fieldAsId) {
                $c = count($names);
                $keys = array_keys($names);
                if ($fieldAsId && isset($values[$field])) {
                    $id = $values[$field];
                }
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
