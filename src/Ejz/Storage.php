<?php

namespace Ejz;

use RuntimeException;
use Generator;
use Error;
use Amp\Loop;
use Amp\Promise;
use Amp\Deferred;
use Amp\Producer;

class Storage
{
    const NO_TABLE_ERROR = 'NO_TABLE_ERROR';
    const INVALID_TABLE_ERROR = 'INVALID_TABLE_ERROR: %s';
    const INVALID_INSERTED_IDS = 'INVALID_INSERTED_IDS: %s / %s';
    const INVALID_SHARDS_FOR_REID = 'INVALID_SHARDS_FOR_REID: %s -> %s';
    const INVALID_ARGUMENT_FORMAT = 'INVALID_ARGUMENT_FORMAT';

    /** @var DatabasePool */
    private $pool;

    /** @var RedisCache */
    private $cache;

    /** @var Bitmap */
    private $bitmap;

    /** @var array */
    private $tables;

    /** @var ?string */
    private $table;

    /** @var array */
    private $args;

    /** @var ?TableDefinition */
    private $definition;

    /**
     * @param DatabasePool $pool
     * @param RedisCache   $cache
     * @param Bitmap       $bitmap
     * @param array        $tables
     * @param ?string      $table  (optional)
     * @param array        $args   (optional)
     */
    public function __construct(
        DatabasePool $pool,
        RedisCache $cache,
        Bitmap $bitmap,
        array $tables,
        ?string $table = null,
        array $args = []
    ) {
        $this->pool = $pool;
        $this->cache = $cache;
        $this->bitmap = $bitmap;
        $this->tables = $tables;
        $this->table = $table;
        $this->args = $args;
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
    public function getTables(): array
    {
        return $this->tables;
    }

    /**
     * @return TableDefinition
     */
    protected function getTableDefinition(): TableDefinition
    {
        if ($this->definition !== null) {
            return $this->definition;
        }
        if ($this->table === null) {
            throw new RuntimeException(self::NO_TABLE_ERROR);
        }
        $table = $this->tables[$this->table] ?? null;
        if ($table === null) {
            throw new RuntimeException(sprintf(self::INVALID_TABLE_ERROR, $this->table));
        }
        $this->definition = new TableDefinition($this->table, $table, $this->getPool()->names());
        return $this->definition;
    }

    /**
     * @param string $table
     * @param array  $args
     *
     * @return self
     */
    public function __call(string $table, array $args): self
    {
        $class = $this->tables[$table]['class'] ?? self::class;
        return new $class($this->pool, $this->cache, $this->bitmap, $this->tables, $table, $args);
    }

    /**
     * @return Promise
     */
    public function createAsync(): Promise
    {
        return \Amp\call(function () {
            $definition = $this->getTableDefinition();
            $shards = $this->getAllShards();
            yield $shards->createAsync($definition);
        });
    }

    /**
     *
     */
    public function create()
    {
        \Amp\Promise\wait($this->createAsync());
    }

    /**
     * @return Promise
     */
    public function dropAsync(): Promise
    {
        return \Amp\call(function () {
            $definition = $this->getTableDefinition();
            $table = $definition->getTable();
            if ($definition->isCacheable()) {
                $this->cache->drop($table);
            }
            $shards = $this->getAllShards();
            yield $shards->dropAsync($table);
        });
    }

    /**
     *
     */
    public function drop()
    {
        \Amp\Promise\wait($this->dropAsync());
    }

    /**
     * @return Promise
     */
    public function truncateAsync(): Promise
    {
        return \Amp\call(function () {
            $definition = $this->getTableDefinition();
            $table = $definition->getTable();
            if ($definition->isCacheable()) {
                $this->cache->drop($table);
            }
            $shards = $this->getAllShards();
            yield $shards->truncateAsync($table);
        });
    }

    /**
     *
     */
    public function truncate()
    {
        \Amp\Promise\wait($this->truncateAsync());
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insertAsync(array $values = []): Promise
    {
        return \Amp\call(function ($values) {
            $deferred = new Deferred();
            $definition = $this->getTableDefinition();
            $shards = $this->getWriteShardsByValues($values);
            $values = $definition->normalizeValues($values);
            $promises = $shards->insertAsync($definition, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
                // var_dump($err);
                $ids = $err ? [0] : array_values($res);
                $min = min($ids);
                $max = max($ids);
                if ($min !== $max) {
                    throw new RuntimeException(sprintf(self::INVALID_INSERTED_IDS, $min, $max));
                }
                $deferred->resolve($min);
            });
            return $deferred->promise();
        }, $values);
    }

    /**
     * @param array $values (optional)
     *
     * @return int
     */
    public function insert(array $values = []): int
    {
        return \Amp\Promise\wait($this->insertAsync($values));
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function updateAsync(int $id, array $values = []): Promise
    {
        return \Amp\call(function ($id, $values) {
            $deferred = new Deferred();
            $definition = $this->getTableDefinition();
            if ($definition->isCacheable()) {
                $table = $definition->getTable();
                $this->cache->drop($table . '.' . $id);
            }
            $values = $definition->normalizeValues($values);
            $shards = $this->getWriteShardsById($id);
            $promises = $shards->updateAsync($definition, $id, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err) use ($deferred) {
                $deferred->resolve(!$err);
            });
            return $deferred->promise();
        }, $id, $values);
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return bool
     */
    public function update(int $id, array $values = []): bool
    {
        return \Amp\Promise\wait($this->updateAsync($id, $values));
    }

    /**
     * @param array ...args
     *
     * @return Promise
     */
    public function getAsync(...$args): Promise
    {
        return \Amp\call(function ($args) {
            $ids = $args;
            $fields = null;
            $last = array_pop($ids);
            if (is_numeric($last)) {
                $ids[] = $last;
            } else {
                $fields = $last;
            }
            if (!$ids) {
                throw new RuntimeException(self::INVALID_ARGUMENT_FORMAT);
            }
            $definition = $this->getTableDefinition();
            $isCacheable = $definition->isCacheable();
            $table = $definition->getTable();
            $result = [];
            $meta = [];
            $dbs = ['ids' => [], 'map' => []];
            foreach ($ids as $id) {
                $v = null;
                if ($isCacheable) {
                    $fields_md5 = $fields_md5 ?? md5(serialize($fields));
                    $ck = $table . '.' . $id . '.' . $fields_md5;
                    $v = $this->cache->get($ck);
                    if ($v !== null) {
                        $result[$id] = $v;
                    } else {
                        $ct = [$table, $table . '.' . $id];
                        $meta[$id] = [$ck, $ct];
                    }
                }
                if ($v === null) {
                    $db = $this->getReadShardsById($id)->random();
                    $name = $db->getName();
                    $dbs['map'][$name] = $db;
                    $dbs['ids'][$name][] = $id;
                }
            }
            if (!$dbs['map']) {
                return $result;
            }
            $fields = $fields ?? array_keys($definition->getFields());
            $fields = array_fill_keys((array) $fields, null);
            $fields = $definition->normalizeValues($fields);
            $promises = [];
            foreach ($dbs['map'] as $name => $db) {
                $promises[] = $db->getAsync($definition, $dbs['ids'][$name], $fields);
            }
            $values = yield $promises;
            foreach (($isCacheable ? $values : []) as $value) {
                foreach ($value as $id => $v) {
                    [$ck, $ct] = $meta[$id];
                    $this->cache->set($ck, $v, 3600, $ct);
                }
            }
            return array_replace($result, ...$values);
        }, $args);
    }

    /**
     * @param array ...$args
     *
     * @return array
     */
    public function get(...$args): array
    {
        return \Amp\Promise\wait($this->getAsync(...$args));
    }

    /**
     * @param int $id1
     * @param int $id2
     *
     * @return Promise
     */
    public function reidAsync(int $id1, int $id2): Promise
    {
        return \Amp\call(function ($id1, $id2) {
            $deferred = new Deferred();
            $definition = $this->getTableDefinition();
            $shards1 = $this->getWriteShardsById($id1);
            $shards2 = $this->getWriteShardsById($id2);
            if (array_diff($shards1->names(), $shards2->names())) {
                throw new RuntimeException(sprintf(self::INVALID_SHARDS_FOR_REID, $id1, $id2));
            }
            if ($definition->isCacheable()) {
                $table = $definition->getTable();
                $this->cache->drop($table . '.' . $id1, $table . '.' . $id2);
            }
            $promises = $shards1->reidAsync($definition, $id1, $id2);
            \Amp\Promise\all($promises)->onResolve(function ($err) use ($deferred) {
                $deferred->resolve(!$err);
            });
            return $deferred->promise();
        }, $id1, $id2);
    }

    /**
     * @param int $id1
     * @param int $id2
     *
     * @return bool
     */
    public function reid(int $id1, int $id2): bool
    {
        return \Amp\Promise\wait($this->reidAsync($id1, $id2));
    }

    /**
     * @param int $id
     *
     * @return Promise
     */
    public function deleteAsync(int $id): Promise
    {
        return \Amp\call(function ($id) {
            $deferred = new Deferred();
            $definition = $this->getTableDefinition();
            if ($definition->isCacheable()) {
                $table = $definition->getTable();
                $this->cache->drop($table . '.' . $id);
            }
            $shards = $this->getWriteShardsById($id);
            $promises = $shards->deleteAsync($definition, $id);
            \Amp\Promise\all($promises)->onResolve(function ($err) use ($deferred) {
                $deferred->resolve(!$err);
            });
            return $deferred->promise();
        }, $id);
    }

    /**
     * @param int $id
     *
     * @return int
     */
    public function delete(int $id): bool
    {
        return \Amp\Promise\wait($this->deleteAsync($id));
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterate(array $params = []): Producer
    {
        $definition = $this->getTableDefinition();
        $table = $definition->getTable();
        $fields = $params['fields'] ?? array_keys($definition->getFields());
        $fields = array_fill_keys((array) $fields, null);
        $params['fields'] = $definition->normalizeValues($fields);
        $values = $params['where'] ?? [];
        $shards = $this->getReadShardsByValues($values);
        $iterators = $shards->iterate($table, $params);
        return $this->joinIterators($iterators, $params);
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterateAsArray(array $params = []): array
    {
        return iterator_to_array($this->iterateAsGenerator($params));
    }

    /**
     * @param array $params (optional)
     *
     * @return Generator
     */
    public function iterateAsGenerator(array $params = []): Generator
    {
        $producer = $this->iterate($params);
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while ($yield = \Amp\Promise\wait(\Amp\call($iterator, $producer))) {
            yield $yield[0] => $yield[1];
        }
    }

    /**
     * @param array $where  (optional)
     * @param mixed $fields (optional)
     *
     * @return array
     */
    public function filter(array $where = [], $fields = null): array
    {
        return $this->iterateAsArray(compact('where', 'fields'));
    }

    /**
     * @return DatabasePool
     */
    public function getAllShards(): DatabasePool
    {
        $definition = $this->getTableDefinition();
        $shards = $definition->getAllShards();
        return $this->getPool()->dbs($shards);
    }

    /**
     * @param int $id
     *
     * @return DatabasePool
     */
    public function getReadShardsById(int $id): DatabasePool
    {
        $definition = $this->getTableDefinition();
        $shards = $definition->getReadShardsById($id);
        return $this->getPool()->dbs($shards);
    }

    /**
     * @param int $id
     *
     * @return DatabasePool
     */
    public function getWriteShardsById(int $id): DatabasePool
    {
        $definition = $this->getTableDefinition();
        $shards = $definition->getWriteShardsById($id);
        return $this->getPool()->dbs($shards);
    }

    /**
     * @param array $values
     *
     * @return DatabasePool
     */
    public function getReadShardsByValues(array $values): DatabasePool
    {
        $definition = $this->getTableDefinition();
        $shards = $definition->getReadShardsByValues($values);
        return $this->getPool()->dbs($shards);
    }

    /**
     * @param array $values
     *
     * @return DatabasePool
     */
    public function getWriteShardsByValues(array $values): DatabasePool
    {
        $definition = $this->getTableDefinition();
        $shards = $definition->getWriteShardsByValues($values);
        return $this->getPool()->dbs($shards);
    }

    /**
     * @param array $iterators
     * @param array $params
     *
     * @return Producer
     */
    private function joinIterators(array $iterators, array $params): Producer
    {
        $emit = function ($emit) use ($iterators, $params) {
            $values = [];
            $ids = [];
            $already = [];
            $asc = $params['asc'] ?? true;
            $rand = $params['rand'] ?? false;
            do {
                $keys = array_keys($iterators);
                foreach ($keys as $key) {
                    if (isset($values[$key])) {
                        continue;
                    }
                    do {
                        if (!yield $iterators[$key]->advance()) {
                            unset($iterators[$key]);
                            continue 2;
                        }
                        $value = $iterators[$key]->getCurrent();
                    } while (isset($already[$value[0]]));
                    $values[$key] = $value;
                    $already[$value[0]] = true;
                    $ids[$value[0]] = $key;
                }
                if ($rand) {
                    @ $id = (int) array_rand($ids);
                } else {
                    $keys = array_keys($ids);
                    @ $id = (int) ($asc ? min($keys) : max($keys));
                }
                while (isset($ids[$id])) {
                    $k = $ids[$id];
                    yield $emit($values[$k]);
                    unset($values[$k]);
                    unset($ids[$id]);
                    $id++;
                }
            } while ($iterators || $values);
        };
        return new Producer($emit);
    }
}