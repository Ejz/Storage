<?php

namespace Ejz;

// use RuntimeException;
// use Generator;
// use Error;
// use Amp\Loop;
// use Amp\Promise;
// use Amp\Deferred;
// use Amp\Producer;

class Storage
{
    protected const INVALID_REPOSITORY_ERROR = 'INVALID_REPOSITORY_ERROR: %s';
    // const NO_TABLE_ERROR = 'NO_TABLE_ERROR';
    // const INVALID_INSERTED_IDS = 'INVALID_INSERTED_IDS: %s / %s';
    // const INVALID_SHARDS_FOR_REID = 'INVALID_SHARDS_FOR_REID: %s -> %s';
    // const INVALID_ARGUMENT_FORMAT = 'INVALID_ARGUMENT_FORMAT';
    // const AMBIGUOUS_WHERE_OR_FIELDS = 'AMBIGUOUS_WHERE_OR_FIELDS';
    // const ALREADY_TABLE_ERROR = 'ALREADY_TABLE_ERROR: %s -> %s';
    // const SORT_HAS_FAILED = 'SORT_HAS_FAILED';

    /** @var DatabasePool */
    protected $pool;

    /** @var RedisCache */
    protected $cache;

    /** @var Bitmap */
    protected $bitmap;

    /** @var array */
    protected $repositories;

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
     * @param array  $args
     *
     * @return Repository
     */
    public function __call(string $repository): Repository
    {
        if (!isset($this->repositories[$repository])) {
            throw new RuntimeException(sprintf(self::INVALID_REPOSITORY_ERROR, $repository));
        }
        return new Repository($this, $this->repositories[$repository]);
    }

    // /**
    //  * @return TableDefinition
    //  */
    // protected function getTableDefinition(): TableDefinition
    // {
    //     if ($this->definition !== null) {
    //         return $this->definition;
    //     }
    //     if ($this->table === null) {
    //         throw new RuntimeException(self::NO_TABLE_ERROR);
    //     }
    //     $table = $this->tables[$this->table] ?? null;
    //     if ($table === null) {
    //         throw new RuntimeException(sprintf(self::INVALID_TABLE_ERROR, $this->table));
    //     }
    //     $this->definition = new TableDefinition($this->table, $table, $this->getPool()->names());
    //     return $this->definition;
    // }

    

    

    

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
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred, $definition, $values) {
                $ids = $err ? [0] : array_values($res);
                $min = min($ids);
                $max = max($ids);
                if ($min !== $max) {
                    throw new RuntimeException(sprintf(self::INVALID_INSERTED_IDS, $min, $max));
                }
                if ($definition->hasBitmap() && $min > 0) {
                    $this->bitmap->upsert($definition, $min, $values);
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
            $bean = $definition->getBean();
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
                    $db = $this->getReadShardsById($id)->filter(function ($name) use ($definition) {
                        return !$definition->isForeignKeyTable($name);
                    })->random();
                    $name = $db->getName();
                    $dbs['map'][$name] = $db;
                    $dbs['ids'][$name][] = $id;
                }
            }
            if (!$dbs['map']) {
                return $bean === null ? $result : $this->toBeans($bean, $result);
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
            $result = array_replace($result, ...$values);
            return $bean === null ? $result : $this->toBeans($bean, $result);
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
     * @param array $ids
     *
     * @return Promise
     */
    public function rotateIdsAsync(...$ids): Promise
    {
        return \Amp\call(function ($ids) {
            if (count($ids) < 2) {
                return false;
            }
            $max = max($ids);
            $ids[] = -$max;
            array_unshift($ids, -$max);
            for ($i = count($ids); $i > 1; $i--) {
                [$id1, $id2] = [$ids[$i - 2], $ids[$i - 1]];
                if (!yield $this->reidAsync($id1, $id2)) {
                    return false;
                }
            }
            return true;
        }, $ids);
    }

    /**
     * @param array ...$args
     *
     * @return bool
     */
    public function rotateIds(...$args): bool
    {
        return \Amp\Promise\wait($this->rotateIdsAsync(...$args));
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
     * @param string $bean
     * @param array  $result
     *
     * @return array
     */
    public function toBeans(string $bean, array $result): array
    {
        $beans = [];
        foreach ($result as $id => $row) {
            $beans[$id] = new $bean($this, $id, $row);
        }
        return $beans;
    }

    /**
     * @param ?callable $score (optional)
     */
    public function sort(?callable $score = null)
    {
        $shards = $this->getAllShards()->names();
        $definition = $this->getTableDefinition();
        foreach ($shards as $shard) {
            $scores = [];
            foreach ($this->iterate(null, null, ['shards' => [$shard]]) as $id => $values) {
                $scores[$id] = $score === null ? $definition->getScore($values) : $score($values);
            }
            $chains = $this->getSwapChains($scores);
            foreach ($chains as $ids) {
                if (!$this->rotateIds(...$ids)) {
                    throw new RuntimeException(self::SORT_HAS_FAILED);
                }
            }
        }
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
     * @param mixed $where  (optional)
     * @param mixed $fields (optional)
     * @param array $params (optional)
     *
     * @return Generator
     */
    public function iterate($where = null, $fields = null, array $params = []): Generator
    {
        if (isset($params['where']) || isset($params['fields'])) {
            throw new RuntimeException(self::AMBIGUOUS_WHERE_OR_FIELDS);
        }
        $definition = $this->getTableDefinition();
        $table = $definition->getTable();
        $fields = $fields ?? array_keys($definition->getFields());
        $fields = array_fill_keys((array) $fields, null);
        $fields = $definition->normalizeValues($fields);
        $shards = $this->getReadShardsByValues(is_string($where) ? [] : ($where ?? []));
        $shards = $shards->filter(function ($name) use ($definition) {
            return !$definition->isForeignKeyTable($name);
        });
        if (isset($params['shards'])) {
            $shards = $shards->dbs($params['shards']);
        }
        $params += compact('where', 'fields');
        $iterators = $shards->iterate($table, $params);
        $producer = $this->joinIterators($iterators, $params);
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while ($yield = \Amp\Promise\wait(\Amp\call($iterator, $producer))) {
            yield from $yield;
        }
    }

    /**
     * @param mixed $where  (optional)
     * @param mixed $fields (optional)
     * @param array $params (optional)
     *
     * @return array
     */
    public function filter($where = null, $fields = null, array $params = []): array
    {
        return iterator_to_array($this->iterate($where, $fields, $params));
    }

    /**
     * @param string $query
     * @param mixed  $fields (optional)
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function index(string $query, $fields = null, array $params = []): Generator
    {
        $params += [
            'limit' => 100,
        ];
        ['limit' => $limit] = $params;
        $definition = $this->getTableDefinition();
        $table = $definition->getTable();
        $ret = $this->bitmap->execute('SEARCH ? ? CURSOR ?', $table, $query, $limit);
        $size = array_shift($ret);
        while ($size > 0) {
            $cursor = $cursor ?? array_shift($ret);
            $ids = $this->bitmap->execute('CURSOR ?', $cursor);
            $c = count($ids);
            $size = $c > 0 ? $size - $c : 0;
            $ids[] = $fields;
            yield from $this->get(...$ids);
        }
    }

    /**
     * @param string $query
     *
     * @return array
     */
    public function getCursor(string $query): array
    {
        $definition = $this->getTableDefinition();
        $table = $definition->getTable();
        return $this->bitmap->execute('SEARCH ? ? WITHCURSOR', $table, $query);
    }

    /**
     * @param string $query
     *
     * @return array
     */
    public function nextCursor(string $cursor, $limit, $fields = null): array
    {
        if ($limit <= 0) {
            return [];
        }
        $ids = $this->bitmap->execute('CURSOR ? LIMIT ?', $cursor, $limit);
        $ids[] = $fields;
        return $this->get(...$ids);
    }
    // $size = array_shift($ret);
    // $cursor = null;
    // if ($size > 0) {
    //     $cursor = array_shift($ret);
    //     $cursor = new BitmapCursor($this, $cursor, $fields);
    // }
    // return [$size, $cursor];

    /**
     * @param string $query
     *
     * @return int
     */
    public function count(string $query): int
    {
        $definition = $this->getTableDefinition();
        $table = $definition->getTable();
        $ret = $this->bitmap->execute('SEARCH ? ? LIMIT ?', $table, $query, 0);
        return (int) array_shift($ret);
    }

    /**
     * @return Promise
     */
    public function maxAsync(): Promise
    {
        return \Amp\call(function () {
            $shards = $this->getAllShards();
            $table = $this->getTableDefinition()->getTable();
            return max($shards->max($table));
        });
    }

    /**
     * @return int
     */
    public function max(): int
    {
        return \Amp\Promise\wait($this->maxAsync());
    }

    /**
     * @return Promise
     */
    public function minAsync(): Promise
    {
        return \Amp\call(function () {
            $shards = $this->getAllShards();
            $table = $this->getTableDefinition()->getTable();
            return min($shards->min($table));
        });
    }

    /**
     * @return int
     */
    public function min(): int
    {
        return \Amp\Promise\wait($this->minAsync());
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
                    yield $emit([$values[$k][0] => $values[$k][1]]);
                    unset($values[$k]);
                    unset($ids[$id]);
                    $id++;
                }
            } while ($iterators || $values);
        };
        return new Producer($emit);
    }

    /**
     * @param array $scores
     *
     * @return array
     */
    private function getSwapChains(array $scores): array
    {
        $ids1 = array_keys($scores);
        arsort($scores);
        $ids2 = array_keys($scores);
        $ids = array_combine($ids1, $ids2);
        $ids = array_filter($ids, function ($v, $k) {
            return $v != $k;
        }, ARRAY_FILTER_USE_BOTH);
        $chains = [];
        while ($ids) {
            $chain = [];
            reset($ids);
            $ex = key($ids);
            do {
                $chain[] = $ex;
                @ $_ = $ids[$ex];
                unset($ids[$ex]);
                @ $ex = $_;
            } while (isset($ids[$ex]));
            $chains[] = array_reverse($chain);
        }
        return $chains;
    }
}
