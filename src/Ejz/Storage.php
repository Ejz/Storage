<?php

namespace Ejz;

use RuntimeException;
use Amp\Promise;

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
     * @return Promise
     */
    public function create(): Promise
    {
        $promises = [];
        foreach (array_keys($this->repositories) as $name) {
            $promises[$name] = $this->$name()->create();
        }
        return Promise\all($promises);
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function createSync(...$args)
    {
        return Promise\wait($this->create(...$args));
    }

    /**
     * @return Promise
     */
    public function drop(): Promise
    {
        $promises = [];
        foreach (array_keys($this->repositories) as $name) {
            $promises[$name] = $this->$name()->drop();
        }
        return Promise\all($promises);
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function dropSync(...$args)
    {
        return Promise\wait($this->drop(...$args));
    }

    /**
     * @return array
     */
    public function sort(): array
    {
        $return = [];
        foreach (array_keys($this->repositories) as $name) {
            $return[$name] = $this->$name()->sort();
        }
        return $return;
    }

    /**
     */
    public function bitmapCreate()
    {
        foreach (array_keys($this->repositories) as $name) {
            $this->$name()->bitmapCreate();
        }
    }

    /**
     */
    public function bitmapDrop()
    {
        foreach (array_keys($this->repositories) as $name) {
            $this->$name()->bitmapDrop();
        }
    }

    /**
     */
    public function bitmapPopulate()
    {
        foreach (array_keys($this->repositories) as $name) {
            $this->$name()->bitmapPopulate();
        }
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
        $getReadablePool = function ($id, $values, $names, $nargs) use ($primary) {
            return [$names[$primary]];
        };
        return compact('isPrimaryTable', 'getReadablePool');
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
