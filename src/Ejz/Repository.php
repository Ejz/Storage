<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use Amp\Iterator;
use RuntimeException;
use Ejz\Type\AbstractType;

class Repository implements NameInterface
{
    use NameTrait;
    use SyncTrait;

    /** @var Storage */
    protected $storage;

    /** @var array */
    protected $config;

    /** @var Pool */
    protected $databasePool;

    /** @var Pool */
    protected $bitmapPool;

    /** @var Pool */
    protected $databaseCluster;

    /** @var Pool */
    protected $bitmapCluster;

    /**
     * @param string  $name
     * @param Storage $storage
     * @param array   $config
     */
    public function __construct(string $name, Storage $storage, array $config)
    {
        $this->setName($name);
        $this->storage = $storage;
        $this->config = $config;
        $this->normalize();
    }

    /**
     * @return Promise
     */
    public function create(): Promise
    {
        if (!empty($this->config['database']['cluster'])) {
            return \Amp\call(function () {
                yield $this->databaseCreate();
                yield $this->bitmapCreate();
            });
        } else {
            return \Amp\call(function () {
                yield $this->databasePool->create($this);
                yield $this->bitmapPool->create($this);
            });
        }
    }

    public function bitmapCreate()
    {
        return \Amp\call(function () {
            $index = $this->getBitmapIndex();
            $fields = $this->getBitmapFields();
            $args = [$index, $fields];
            //
            $primaryPool = $this->getBitmapPool(Pool::POOL_PRIMARY);
            $promises = $primaryPool->createNew($index, $fields);
            yield $promises;            
        });
    }

    public function databaseCreate()
    {
        return \Amp\call(function () {
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            $fields = $this->getDatabaseFields();
            $indexes = $this->getDatabaseIndexes();
            $foreignKeys = $this->getDatabaseForeignKeys();
            $args = [
                $table, $primaryKey,
                3 => 0, 4 => 0,
                5 => $fields, 6 => $indexes,
                $foreignKeys,
            ];
            foreach ([Pool::POOL_PRIMARY, Pool::POOL_SECONDARY] as $type) {
                $isSecondary = $type === Pool::POOL_SECONDARY;
                $pool = $this->getDatabasePool($type, null);
                $names = $pool->names();
                yield $pool->each(function ($db) use ($names, $args, $isSecondary) {
                    $primaryKeyStart = array_search($db->getName(), $names) + 1;
                    $primaryKeyIncrement = count($names);
                    [$args[3], $args[4]] = [$primaryKeyStart, $primaryKeyIncrement];
                    if ($isSecondary) {
                        [$args[5], $args[6]] = [[], []];
                    }
                    return $db->createNew(...$args);
                });
            }
        });
    }

    /**
     * @return Promise
     */
    public function drop(): Promise
    {
        return \Amp\call(function () {
            yield $this->databasePool->drop($this->getDatabaseTable());
            yield $this->bitmapPool->drop($this->getBitmapIndex());
        });
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insert(array $values = []): Promise
    {
        $bean = $this->getDatabaseBeanWithValues(null, $values);
        return $this->insertBean($bean);
    }

    /**
     * @param DatabaseBean $bean
     *
     * @return Promise
     */
    public function insertBean(DatabaseBean $bean): Promise
    {
        $deferred = new Deferred();
        if (!empty($this->config['database']['cluster'])) {
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            $fields = $bean->getFields();
            $promises = [];
            $pools = [];
            foreach ([Pool::POOL_PRIMARY, Pool::POOL_SECONDARY] as $type) {
                $isSecondary = $type === Pool::POOL_SECONDARY;
                $pool = $this->getDatabasePool($type | Pool::POOL_RANDOM, null);
                $_ = $isSecondary ? [] : $fields;
                $promises[] = Promise\any($pool->insertNew($table, $primaryKey, $_));
                $pools[] = $pool;
            }
            $promise = Promise\all($promises);
            $promise->onResolve(function ($e, $v) use ($deferred, $pools, $table, $primaryKey) {
                $id = null;
                $collect = [];
                foreach ($v as $idx => [$err, $ids]) {
                    $collect[] = ['pool' => $pools[$idx], 'ids' => $ids];
                    $min = $ids ? min($ids) : 0;
                    $max = $ids ? max($ids) : 0;
                    if ($err || !$min || !$max || $min !== $max) {
                        $id = 0;
                    }
                    if ($id === 0) {
                        continue;
                    }
                    if ($min !== $max) {
                        $id = 0;
                    }
                }
                foreach (($id ? [] : $collect) as ['pool' => $pool, 'ids' => $ids]) {
                    $pool->deleteNewSync($table, $primaryKey, $ids);
                }
                $deferred->resolve($id);
            });
            return $deferred->promise();

            $promise->onResolve(function ($e, $v) use ($deferred) {
                $deferred->resolve($min === $max ? $min : null);
            });
            return $promise;
            //
            $primaryPool = $this->getDatabasePool(Pool::POOL_PRIMARY, $bean);
            $promises = $primaryPool->insertNew($table, $primaryKey, $fields);
            $promise = Promise\all($promises);
            //
            
            $promise->onResolve(function ($err, $res) use ($deferred, $primaryPool) {
                $ids = $err ? [0] : ($res ?: [0]);
                $min = min($ids);
                $max = max($ids);
                $deferred->resolve($min === $max ? $min : null);
            });

            $names = $primaryPool->names();
            $promises = $primaryPool->each(function ($db) use ($names, $args) {
                $primaryKeyStart = array_search($db->getName(), $names) + 1;
                $primaryKeyIncrement = count($names);
                [$args[3], $args[4]] = [$primaryKeyStart, $primaryKeyIncrement];
                return $db->createNew(...$args);
            });
            yield $promises;
            //
            $secondaryPool = $this->getDatabasePool(Pool::POOL_SECONDARY);
            $names = $secondaryPool->names();
            $promises = $secondaryPool->each(function ($db) use ($names, $args) {
                $primaryKeyStart = array_search($db->getName(), $names) + 1;
                $primaryKeyIncrement = count($names);
                [$args[3], $args[4]] = [$primaryKeyStart, $primaryKeyIncrement];
                [$args[5], $args[6]] = [[], []];
                return $db->createNew(...$args);
            });
            yield $promises;

            return \Amp\call(function () {
                
            });
        } else {
            $pool = $this->getWritableDatabasePool($bean);
            $promises = $pool->insert($this, $bean->getFields());
            Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
                $ids = $err ? [0] : ($res ?: [0]);
                $min = min($ids);
                $max = max($ids);
                $deferred->resolve($min === $max ? $min : null);
            });
        }
        return $deferred->promise();
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function add(int $id, array $values = []): Promise
    {
        $bean = $this->getBitmapBeanWithValues($id, $values);
        return $this->addBean($bean);
    }

    /**
     * @param BitmapBean $bean
     *
     * @return Promise
     */
    public function addBean(BitmapBean $bean): Promise
    {
        $deferred = new Deferred();
        $pool = $this->getWritableBitmapPool($bean->id);
        $index = $this->getBitmapIndex();
        // $shard = ->random()->getName();
        $_ = $this->getWritableDatabasePool($bean->id);
        $promises = $pool->add($index, $bean->id, $bean->getFields(), $_);
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $ids = $err ? [0] : ($res ?: [0]);
            $min = min($ids);
            $max = max($ids);
            $deferred->resolve($min === $max ? $min : null);
        });
        return $deferred->promise();
    }

    /**
     * @param array $ids
     *
     * @return Emitter
     */
    public function get(array $ids): Emitter
    {
        $emitter = new Emitter();
        $coroutine = \Amp\call(function ($ids, $emitter) {
            $table = $this->getDatabaseTable();
            $dbs = [];
            $meta = [];
            $cached = [];
            foreach ($ids as $id) {
                if ($this->config['cache'] !== null) {
                    $cache = $cache ?? $this->storage->getCache();
                    $ck = $table . '.' . $id;
                    $v = $cache->get($ck);
                    if ($v !== null) {
                        $cached[] = $v;
                        continue;
                    }
                    $ct = [$table, $table . '.' . $id];
                    $meta[$id] = [$ck, $ct];
                }
                $db = $this->getReadableDatabasePool($id)->random();
                $name = $db->getName();
                $dbs[$name] = $dbs[$name] ?? ['db' => $db, 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
            $iterators = [Emitter::fromIterable($cached)];
            foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
                $params = $params ?? [
                    'pk' => [$this->getDatabasePk()],
                    'fields' => array_values($this->getDatabaseFields()),
                    'returnFields' => true,
                ];
                $iterators[] = $db->get($table, $ids, $params);
            }
            if (count($iterators) > 1) {
                $iterator = Emitter::merge($iterators);
            } else {
                [$iterator] = $iterators;
            }
            while (($value = yield $iterator->pull()) !== null) {
                [$id, $fields] = $value;
                $bean = $this->getDatabaseBeanWithFields($id, $fields);
                if (isset($meta[$id])) {
                    [$ck, $ct] = $meta[$id];
                    $cache = $cache ?? $this->storage->getCache();
                    $cache->set($ck, $value, $this->config['cache']['ttl'], $ct);
                    foreach ($this->config['cache']['fieldsToId'] as $f2id) {
                        $v = md5(serialize($bean->$f2id));
                        $ck = $table . '.' . $f2id . '.' . $v;
                        $cache->set($ck, $id, $this->config['cache']['ttl'], $ct);
                    }
                }
                yield $emitter->push([$id, $bean]);
            }
        }, $ids, $emitter);
        $coroutine->onResolve(function () use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
    }

    /**
     * @param array $ids
     * @param array $fields
     *
     * @return Promise
     */
    public function update(array $ids, array $fields): Promise
    {
        return $this->updateDelete($ids, $fields);
    }

    /**
     * @param array $ids
     *
     * @return Promise
     */
    public function delete(array $ids): Promise
    {
        return $this->updateDelete($ids, null);
    }

    /**
     * @param array  $ids
     * @param ?array $fields
     *
     * @return Promise
     */
    private function updateDelete(array $ids, ?array $fields): Promise
    {
        $deferred = new Deferred();
        $dbs = [];
        foreach ($ids as $id) {
            if ($this->config['cache'] !== null) {
                $table = $table ?? $this->getDatabaseTable();
                $cache = $cache ?? $this->storage->getCache();
                $cache->drop($table . '.' . $id);
            }
            $pool = $this->getWritableDatabasePool($id);
            foreach ($pool->names() as $name) {
                $dbs[$name] = $dbs[$name] ?? ['db' => $pool->instance($name), 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
        }
        $promises = [];
        foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
            if ($fields === null) {
                $promises[] = $db->delete($this, $ids);
            } else {
                $promises[] = $db->update($this, $ids, $fields);
            }
        }
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $deferred->resolve($err ? 0 : array_sum($res));
        });
        return $deferred->promise();
    }

    /**
     * @param array $params (optional)
     *
     * @return Emitter
     */
    public function iterate(array $params = []): Emitter
    {
        $emitter = new Emitter();
        $coroutine = \Amp\call(function ($params, $emitter) {
            $params = [
                'pk' => [$this->getDatabasePk()],
                'fields' => array_values($this->getDatabaseFields()),
                'returnFields' => true,
            ] + $params;
            $poolFilter = $params['poolFilter'] ?? null;
            unset($params['poolFilter']);
            $table = $this->getDatabaseTable();
            $pool = $this->getReadableDatabasePool();
            if ($poolFilter !== null) {
                $pool = $pool->filter($poolFilter);
            }
            $iterators = [];
            foreach ($pool->names() as $name) {
                $iterators[] = $pool->instance($name)->iterate($table, $params);
            }
            if (count($iterators) > 1) {
                $iterator = Emitter::merge($iterators);
            } else {
                [$iterator] = $iterators;
            }
            $ids = [];
            while (($value = yield $iterator->pull()) !== null) {
                [$id, $fields] = $value;
                if (isset($ids[$id])) {
                    continue;
                }
                $ids[$id] = true;
                $bean = $this->getDatabaseBeanWithFields($id, $fields);
                yield $emitter->push([$id, $bean]);
            }
        }, $params, $emitter);
        $coroutine->onResolve(function () use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
        // $emit = function ($emit) use ($params) {
            
        // };
        // return new Producer($emit);
    }

    /**
     */
    private function normalize()
    {
        $is_assoc = function ($array) {
            if (!is_array($array)) {
                return false;
            }
            $count0 = count($array);
            $count1 = count(array_filter(array_keys($array), 'is_string'));
            return $count0 === $count1;
        };
        //
        //
        //
        $this->config['hasDatabase'] = isset($this->config['database']);
        $this->config['hasBitmap'] = isset($this->config['bitmap']);
        //
        //
        //
        $this->config['database']['cluster'] = $this->config['database']['cluster'] ?? '';
        $this->config['bitmap']['cluster'] = $this->config['bitmap']['cluster'] ?? '';
        //
        //
        //
        $_ = strtolower($this->name);
        $this->config['database']['table'] = $this->config['database']['table'] ?? $_;
        $this->config['bitmap']['index'] = $this->config['bitmap']['index'] ?? $_;
        //
        //
        //        
        $_ = $this->config['database']['pk'] ?? '%s_id';
        $this->config['database']['pk'] = sprintf($_, $this->config['database']['table']);
        //
        //
        //
        if (!$this->config['hasDatabase']) {
            $this->databasePool = new Pool([]);
        } else {
            $databasePool = $this->storage->getDatabasePool();
            $filter = $this->config['database']['poolFilter'] ?? null;
            unset($this->config['database']['poolFilter']);
            if ($filter !== null) {
                $databasePool = $databasePool->filter($filter);
            }
            $this->databasePool = $databasePool;
        }
        //
        //
        //
        if (!$this->config['hasBitmap']) {
            $this->bitmapPool = new Pool([]);
        } else {
            $bitmapPool = $this->storage->getBitmapPool();
            $filter = $this->config['bitmap']['poolFilter'] ?? null;
            unset($this->config['bitmap']['poolFilter']);
            if ($filter !== null) {
                $bitmapPool = $bitmapPool->filter($filter);
            }
            $this->bitmapPool = $bitmapPool;
        }
        //
        //
        //
        $cache = $this->config['cache'] ?? null;
        if ($cache !== null) {
            $cache += [
                'ttl' => 3600,
            ];
            @ $cache['fieldsToId'] = (array) $cache['fieldsToId'];
        }
        $this->config['cache'] = $cache;
        //
        //
        //
        $this->config['bitmap']['handleValues'] = $this->config['bitmap']['handleValues'] ?? null;
        //
        //
        //
        $fields = $this->config['bitmap']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $collect[$name] = $field;
        }
        $this->config['bitmap']['fields'] = $collect;
        //
        //
        //
        $fields = $this->config['database']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $collect[$name] = $field;
        }
        $this->config['database']['fields'] = $collect;
        //
        //
        //
        $indexes = $this->config['database']['indexes'] ?? [];
        $collect = [];
        foreach ($indexes as $name => $index) {
            if (!$is_assoc($index)) {
                $index = ['fields' => $index];
            }
            $type = $index['type'] ?? null;
            $collect[] = new Index($name, $index['fields'], $type);
        }
        $this->config['database']['indexes'] = $collect;
        //
        //
        //
        $foreignKeys = $this->config['database']['foreignKeys'] ?? [];
        $collect = [];
        foreach ($foreignKeys as $name => $foreignKey) {
            if (is_string($foreignKey)) {
                [$t, $f] = explode('.', $foreignKey);
                $foreignKey = [
                    'childFields' => explode(',', $f),
                    'parentFields' => explode(',', $f),
                    'parentTable' => $t,
                ];
            }
            $collect[] = new ForeignKey(
                $name,
                (array) $foreignKey['childFields'],
                $foreignKey['parentTable'],
                (array) $foreignKey['parentFields']
            );
        }
        $this->config['database']['foreignKeys'] = $collect;
    }

    /**
     * @param array $conditions
     *
     * @return Emitter
     */
    public function filter(array $conditions): Emitter
    {
        if (
            $this->config['cache'] !== null &&
            count($conditions) === 1 &&
            in_array($f = key($conditions), $this->config['cache']['fieldsToId'])
        ) {
            $value = current($conditions);
            if (!is_array($value)) {
                $table = $this->getDatabaseTable();
                $v = md5(serialize($value));
                $ck = $table . '.' . $f . '.' . $v;
                $id = $this->storage->getCache()->get($ck);
                if ($id !== null) {
                    return $this->get([$id]);
                }
            }
        }
        $where = new Condition($conditions);
        return $this->iterate(compact('where'));
    }

    /**
     * @param array $conditions
     *
     * @return Promise
     */
    public function exists(array $conditions): Promise
    {
        return \Amp\call(function ($conditions) {
            $value = yield $this->filter($conditions)->pull();
            return $value !== null;
        }, $conditions);
    }

    /**
     * @param int $id1
     * @param int $id2
     *
     * @return Promise
     */
    public function reid(int $id1, int $id2): Promise
    {
        $pool1 = $this->getWritableDatabasePool($id1);
        $pool2 = $this->getWritableDatabasePool($id2);
        $names1 = $pool1->names();
        $names2 = $pool2->names();
        if (!$names1 || array_diff($names1, $names2)) {
            return new Success(false);
        }
        if ($this->config['cache'] !== null) {
            $table = $this->getDatabaseTable();
            $cache = $this->storage->getCache();
            $cache->drop($table . '.' . $id1);
        }
        $deferred = new Deferred();
        $promises = $pool1->reid($this, $id1, $id2);
        Promise\all($promises)->onResolve(function ($err) use ($deferred) {
            $deferred->resolve(!$err);
        });
        return $deferred->promise();
    }

    /**
     * @return bool
     */
    public function sort(): bool
    {
        $ok = 0;
        $names = $this->getReadableDatabasePool()->names();
        foreach ($names as $name) {
            $scores = [];
            $generator = $this->iterate(['poolFilter' => $name])->generator();
            foreach ($generator as $id => $bean) {
                $scores[$id] = $this->getSortScore($bean);
            }
            foreach ($this->getSortChains($scores) as $ids) {
                $max = max($ids);
                $ids[] = -$max;
                array_unshift($ids, -$max);
                for ($i = count($ids); $i > 1; $i--) {
                    [$id1, $id2] = [$ids[$i - 2], $ids[$i - 1]];
                    if (!$this->reidSync($id1, $id2)) {
                        break 2;
                    }
                }
            }
            $ok++;
        }
        return $ok === count($names);
    }

    /**
     */
    public function bitmapPopulate()
    {
        $this->bitmapPool->dropSync($this->getBitmapIndex());
        $this->bitmapPool->createSync($this);
        foreach ($this->iterate()->generator() as $bean) {
            $this->getBitmapBeanFromDatabaseBean($bean)->addSync();
        }
    }

    /**
     * @param string|array $query
     *
     * @return Emitter
     */
    public function search($query): Emitter
    {
        $db = $this->getReadableDatabasePool();
        $pool = $this->getReadableBitmapPool();
        $bitmap = $pool->random();
        $iterators = [];
        $size = 0;
        foreach ($db->names() as $name) {
            if (is_string($query)) {
                $q = "({$query}) @_shard:{$name}";
            } else {
                $q = $query[$name];
            }
            $iterator = $bitmap->search($this, $q);
            $size += $iterator->getSize();
            $iterators[$name] = $iterator;
        }
        $iterator = $this->getSearchIterator($iterators);
        $iterator->setSize($size);
        return $iterator;
    }

    // *
    //  * @param array  $iterators
    //  * @param string $cursor
    //  *
    //  * @return Emitter
     
    // public function getSearchIterator(array $iterators): Emitter
    // {
        
    // }

    /**
     * @param array $scores
     *
     * @return array
     */
    private function getSortChains(array $scores): array
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

    // /**
    //  * @param string $name
    //  * @param array  $arguments
    //  *
    //  * @return mixed
    //  */
    // public function __call(string $name, array $arguments)
    // {
    //     $closure = function ($one, $two) {
    //         return function ($smth = null) use ($one, $two) {
    //             $filter = $this->config[$one][$two . 'PoolFilter'] ?? null;
    //             $pool = $one . 'Pool';
    //             $pool = $this->$pool;
    //             if ($filter === null) {
    //                 return $pool;
    //             }
    //             return $pool->filter($filter($smth, $pool->names()));
    //         };
    //     };
    //     $this->_map = $this->_map ?? [
    //         'hasDatabase' => function () {
    //             return $this->config['hasDatabase'];
    //         },
    //         'hasBitmap' => function () {
    //             return $this->config['hasBitmap'];
    //         },
    //         'getDatabaseTable' => function () {
    //             return $this->config['database']['table'];
    //         },
    //         'getBitmapIndex' => function () {
    //             return $this->config['bitmap']['index'];
    //         },
    //         //
    //         'getDatabaseFields' => function () {
    //             $fields = [];
    //             foreach ($this->config['database']['fields'] as $name => $field) {
    //                 $fields[$name] = new Field($name, $field['type']);
    //             }
    //             return $fields;
    //         },
    //         'getBitmapFields' => function () {
    //             $fields = [];
    //             foreach ($this->config['bitmap']['fields'] as $name => $field) {
    //                 $fields[$name] = new Field($name, $field['type']);
    //             }
    //             return $fields;
    //         },
    //         'getSortScore' => function ($bean) {
    //             $getSortScore = $this->config['database']['getSortScore'] ?? null;
    //             return $getSortScore !== null ? $getSortScore($bean) : 0;
    //         },
    //         'getDatabasePk' => function () {
    //             return $this->config['database']['pk'];
    //         },
    //         'getDatabasePkIncrementBy' => function ($name) {
    //             $get = $this->config['database']['getPkIncrementBy'] ?? null;
    //             if ($get === null) {
    //                 return 1;
    //             }
    //             return (int) $get($name, $this->databasePool->names());
    //         },
    //         'getDatabasePkStartWith' => function ($name) {
    //             $get = $this->config['database']['getPkStartWith'] ?? null;
    //             if ($get === null) {
    //                 return 1;
    //             }
    //             return (int) $get($name, $this->databasePool->names());
    //         },
    //         'getDatabaseIndexes' => function () {
    //             return $this->config['database']['indexes'];
    //         },
    //         'getDatabaseForeignKeys' => function () {
    //             return $this->config['database']['foreignKeys'];
    //         },
    //         //
    //         'getPrimaryDatabasePool' => $closure('database', 'primary'),
    //         'getWritableDatabasePool' => $closure('database', 'writable'),
    //         'getReadableDatabasePool' => $closure('database', 'readable'),
    //         'getPrimaryBitmapPool' => $closure('bitmap', 'primary'),
    //         'getWritableBitmapPool' => $closure('bitmap', 'writable'),
    //         'getReadableBitmapPool' => $closure('bitmap', 'readable'),
    //         //
    //         'getDatabaseBean' => function ($id = null, $values = []) {
    //             return $this->getDatabaseBeanWithValues($id, $values);
    //         },
    //         'getDatabaseBeanWithValues' => function ($id, $values) {
    //             $bean = $this->getDatabaseBeanWithFields($id, $this->getDatabaseFields());
    //             $bean->setValues($values);
    //             return $bean;
    //         },
    //         'getDatabaseBeanWithFields' => function ($id, $fields) {
    //             $bean = $this->config['database']['bean'] ?? DatabaseBean::class;
    //             return new $bean($this, $id, $fields);
    //         },
    //         'getBitmapBean' => function ($id, $values = []) {
    //             return $this->getBitmapBeanWithValues($id, $values);
    //         },
    //         'getBitmapBeanWithValues' => function ($id, $values) {
    //             $bean = $this->getBitmapBeanWithFields($id, $this->getBitmapFields());
    //             $bean->setValues($values);
    //             return $bean;
    //         },
    //         'getBitmapBeanWithFields' => function ($id, $fields) {
    //             $bean = $this->config['bitmap']['bean'] ?? BitmapBean::class;
    //             return new $bean($this, $id, $fields);
    //         },
    //         'getBitmapBeanFromDatabaseBean' => function ($bean) {
    //             $id = (int) $bean->id;
    //             $getValues = $this->config['bitmap']['getValues'] ?? null;
    //             if ($getValues === null) {
    //                 $keys = array_keys($this->config['bitmap']['fields']);
    //                 $values = array_intersect_key($bean->getValues(), array_flip($keys));
    //             } else {
    //                 $values = $getValues($bean);
    //             }
    //             return $this->getBitmapBeanWithValues($id, $values);
    //         },
    //     ];
    //     $method = $this->_map[$name] ?? null;
    //     if ($method === null) {
    //         throw new RuntimeException(sprintf(self::METHOD_NOT_FOUND, $name));
    //     }
    //     return $method(...$arguments);
    // }
    
    /**
     * @return Storage
     */
    public function getStorage(): Storage
    {
        return $this->storage;
    }

    /**
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @return Pool
     */
    public function getDatabasePool(): Pool
    {
        $args = func_get_args();
        if ($args) {
            [$flags, $object] = [$args[0], $args[1] ?? null];
            if (!is_object($object)) {
                $object = (object) ['id' => $object];
            }
            $names = $this->databasePool->names();
            if ($this->databaseCluster === null) {
                $cluster = $this->config['database']['cluster'];
                $cluster = Pool::POOL_CLUSTER_DEFAULT_S . $cluster;
                $cluster = Pool::POOL_CLUSTER_DEFAULT_P . $cluster;
                $cluster = Pool::POOL_CLUSTER_DEFAULT_W . $cluster;
                preg_match_all('~(\w):(.*?)(;|$)~', $cluster, $matches, PREG_SET_ORDER);
                $collect = [];
                foreach ($matches as $match) {
                    $collect[$match[1]] = $match[2];
                }
                $filter = Pool::convertNotationToFilter($collect['w']);
                $this->databaseCluster[Pool::POOL_WRITABLE] = $_ = $this->databasePool->filter($filter);
                $filter = Pool::convertNotationToFilter($collect['p']);
                $this->databaseCluster[Pool::POOL_PRIMARY] = $_->filter($filter);
                $filter = Pool::convertNotationToFilter($collect['p'], true);
                $this->databaseCluster[Pool::POOL_SECONDARY] = $_->filter($filter);
                $this->databaseCluster['s'] = explode(':', $collect['s']);
                $this->databaseCluster['s'][0] = (int) $this->databaseCluster['s'][0];
                $this->databaseCluster['s'][1] = explode(',', $this->databaseCluster['s'][1]);
            }
            $pool = $this->databaseCluster[$flags & (~Pool::POOL_RANDOM)];
            if (!$pool->names()) {
                return $pool;
            }
            $s = $this->databaseCluster['s'];
            foreach ($s[1] as $field) {
                $v = $object->$field ?? null;
                if ($v !== null) {
                    $names = $pool->names();
                    $c = count($names);
                    $v = is_numeric($v) ? abs($v) : crc32($v);
                    $v = ($v % $c) - 1;
                    $v = $v < 0 ? $v + $c : $v;
                    $collect = [];
                    for ($i = 0; $i < $s[0]; $i++) {
                        $collect[] = $names[($v + $i) % $c];
                    }
                    $pool = $pool->filter($collect);
                }
            }
            if (($flags & Pool::POOL_RANDOM) && ($names = $pool->names())) {
                $v = array_rand($names);
                $collect = [];
                for ($i = 0; $i < $s[0]; $i++) {
                    $collect[] = $names[($v + $i) % $c];
                }
                $pool = $pool->filter($collect);
            }
            return $pool;
        }
        return $this->databasePool;
    }

    /**
     * @return Pool
     */
    public function getBitmapPool(): Pool
    {
        return $this->bitmapPool;
    }

    public function getDatabasePrimaryKey(): string
    {
        return $this->config['database']['pk'];
    }
}
