<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use Amp\Iterator;
use RuntimeException;
use Ejz\Type\AbstractType;

class Repository
{
    /** @var Storage */
    protected $storage;

    /** @var string */
    protected $name;

    /** @var array */
    protected $config;

    /** @var Pool */
    protected $databasePool;

    /** @var Pool */
    protected $bitmapPool;

    /** @var string */
    private const METHOD_NOT_FOUND = 'METHOD_NOT_FOUND: %s';

    /**
     * @param Storage $storage
     * @param string  $name
     * @param array   $config
     */
    public function __construct(Storage $storage, string $name, array $config)
    {
        $this->storage = $storage;
        $this->name = $name;
        $this->config = $config;
        $this->normalize();
    }

    /**
     * @return Promise
     */
    public function create(): Promise
    {
        return \Amp\call(function () {
            yield $this->databasePool->create($this);
            yield $this->bitmapPool->create($this);
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
        $pool = $this->getWritableDatabasePool($bean);
        $promises = $pool->insert($this, $bean->getFields());
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
     * @return Iterator
     */
    public function get(array $ids): Iterator
    {
        $emit = function ($emit) use ($ids) {
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
            $iterators = [Iterator\fromIterable($cached)];
            foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
                $params = $params ?? [
                    'pk' => [$this->getDatabasePk()],
                    'fields' => array_values($this->getDatabaseFields()),
                    'returnFields' => true,
                ];
                $iterators[] = $db->get($table, $ids, $params);
            }
            $iterator = Iterator\merge($iterators);
            while (yield $iterator->advance()) {
                [$id, $fields] = $value = $iterator->getCurrent();
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
                yield $emit([$id, $bean]);
            }
        };
        return new Producer($emit);
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

    // /**
    //  * @param array $values (optional)
    //  *
    //  * @return bool
    //  */
    // public function bitmapInsert(int $id, array $values = []): Promise
    // {
    //     $bean = $this->getBitmapBean($id, $values);
    //     return $this->bitmapInsertBean($bean);
    // }

    // /**
    //  * @param BitmapBean $bean
    //  *
    //  * @return Promise
    //  */
    // public function bitmapInsertBean(BitmapBean $bean): Promise
    // {
    //     $id = $bean->id;
    //     $deferred = new Deferred();
    //     $index = $this->getBitmapIndex();
    //     $pool = $this->getWritableBitmapPool($id, $bean->getValues());
    //     $promises = $pool->insert($index, $id, $bean->getFields());
    //     Promise\all($promises)->onResolve(function ($err) use ($deferred, $id) {
    //         $deferred->resolve($err ? 0 : $id);
    //     });
    //     return $deferred->promise();
    // }

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
        $_ = strtolower($this->name);
        $this->config['database']['table'] = $this->config['database']['table'] ?? $_;
        $this->config['bitmap']['index'] = $this->config['bitmap']['index'] ?? $_;
        //
        //
        //
        $_ = $this->config['database']['table'] . '_id';
        $this->config['database']['pk'] = $this->config['database']['pk'] ?? $_;
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

    // /**
    //  * @param array $params (optional)
    //  *
    //  * @return Iterator
    //  */
    // public function iterate(array $params = []): Iterator
    // {
    //     $emit = function ($emit) use ($params) {
    //         $params = [
    //             'pk' => [$this->getPk()],
    //             'fields' => array_values($this->getDatabaseFields()),
    //             'returnFields' => true,
    //         ] + $params;
    //         $poolFilter = $params['poolFilter'] ?? null;
    //         unset($params['poolFilter']);
    //         $table = $this->getTable();
    //         $pool = $this->getReadablePool();
    //         if ($poolFilter !== null) {
    //             $pool = $pool->filter($poolFilter);
    //         }
    //         $iterators = [];
    //         foreach ($pool->names() as $name) {
    //             $iterators[] = $pool->instance($name)->iterate($table, $params);
    //         }
    //         $iterator = Iterator\merge($iterators);
    //         $ids = [];
    //         while (yield $iterator->advance()) {
    //             [$id, $fields] = $iterator->getCurrent();
    //             if (isset($ids[$id])) {
    //                 continue;
    //             }
    //             $ids[$id] = true;
    //             $bean = $this->getBeanWithFields($id, $fields);
    //             yield $emit([$id, $bean]);
    //         }
    //     };
    //     return new Producer($emit);
    // }

    // /**
    //  * @param array $conditions
    //  *
    //  * @return Iterator
    //  */
    // public function filter(array $conditions): Iterator
    // {
    //     if (
    //         $this->cache !== null &&
    //         count($conditions) === 1 &&
    //         in_array($f = key($conditions), $this->cache['fieldsToId'])
    //     ) {
    //         $value = current($conditions);
    //         if (!is_array($value)) {
    //             $table = $this->getTable();
    //             $v = md5(serialize($value));
    //             $ck = $table . '.' . $f . '.' . $v;
    //             $id = $this->storage->getCache()->get($ck);
    //             if ($id !== null) {
    //                 return $this->get([$id]);
    //             }
    //         }
    //     }
    //     $where = new Condition($conditions);
    //     return $this->iterate(compact('where'));
    // }

    // /**
    //  * @param array $conditions
    //  *
    //  * @return Promise
    //  */
    // public function exists(array $conditions): Promise
    // {
    //     return $this->filter($conditions)->advance();
    // }

    // /**
    //  * @param string $query
    //  *
    //  * @return Iterator
    //  */
    // public function search(string $query): Iterator
    // {
    //     [$size, $cursor] = $this->bitmapSearch($query);
    //     $iterator_chunk_size = $this->config['iterator_chunk_size'];
    //     $emit = function ($emit) use ($size, $cursor, $iterator_chunk_size) {
    //         while ($size > 0) {
    //             $limit = min($size, $iterator_chunk_size);
    //             $iterator = $this->bitmapIterator($cursor, $limit);
    //             while (yield $iterator->advance()) {
    //                 yield $emit($iterator->getCurrent());
    //                 $size--;
    //             }
    //         }
    //     };
    //     $iterator = new Producer($emit);
    //     $iterator->setSize($size);
    //     return $iterator;
    // }

    // /**
    //  * @param string $cursor
    //  * @param int    $limit
    //  *
    //  * @return Iterator
    //  */
    // public function bitmapIterator(string $cursor, int $limit): Iterator
    // {
    //     $inc = 0;
    //     $getSortScore = $this->config['getSortScore'] ?? null;
    //     if ($getSortScore !== null) {
    //         $inc = count($this->pool->names());
    //     }
    //     $ids = $this->storage->getBitmap()->CURSOR($cursor, 'LIMIT', $limit + $inc);
    //     $iterators = [];
    //     foreach ($ids as $id) {
    //         $db = $this->getReadablePool($id)->random();
    //         $name = $db->getName();
    //         $iterators[$name] = $iterators[$name] ?? [];
    //         $iterators[$name][] = $id;
    //     }
    //     $iterators = array_map(function ($ids) {
    //         $iterator = $this->get($ids);
    //         $iterator = Producer::getIteratorWithIdsOrder($iterator, $ids);
    //         return $iterator;
    //     }, $iterators);
    //     $iterator = Producer::getIteratorWithSortedValues($iterators, $getSortScore);
    //     if ($inc > 0) {
    //         $current = 0;
    //         $iterator = Iterator\filter($iterator, function () use (&$current, $limit) {
    //             $current++;
    //             return $current <= $limit;
    //         });
    //     }
    //     return $iterator;
    // }

    

    // /**
    //  * @param int $id1
    //  * @param int $id2
    //  *
    //  * @return Promise
    //  */
    // public function reid(int $id1, int $id2): Promise
    // {
    //     $pool1 = $this->getWritableDatabasePool($id1);
    //     $pool2 = $this->getWritableDatabasePool($id2);
    //     $names1 = $pool1->names();
    //     $names2 = $pool2->names();
    //     if (!$names1 || array_diff($names1, $names2)) {
    //         return new Success(false);
    //     }
    //     $deferred = new Deferred();
    //     $promises = $pool1->reid($this, $id1, $id2);
    //     Promise\all($promises)->onResolve(function ($err) use ($deferred) {
    //         $deferred->resolve(!$err);
    //     });
    //     return $deferred->promise();
    // }

    // /**
    //  * @return bool
    //  */
    // public function sort(): bool
    // {
    //     $getSortScore = $this->config['getSortScore'] ?? null;
    //     if ($getSortScore === null) {
    //         return false;
    //     }
    //     $names = $this->pool->names();
    //     $ok = 0;
    //     foreach ($names as $name) {
    //         $scores = [];
    //         $generator = $this->iterate(['poolFilter' => $name])->generator();
    //         foreach ($generator as $id => $bean) {
    //             $scores[$id] = $getSortScore($bean);
    //         }
    //         foreach ($this->getSortChains($scores) as $ids) {
    //             $max = max($ids);
    //             $ids[] = -$max;
    //             array_unshift($ids, -$max);
    //             for ($i = count($ids); $i > 1; $i--) {
    //                 [$id1, $id2] = [$ids[$i - 2], $ids[$i - 1]];
    //                 if (!$this->reidSync($id1, $id2)) {
    //                     break 2;
    //                 }
    //             }
    //         }
    //         $ok++;
    //     }
    //     return $ok === count($names);
    // }

    // /**
    //  * @param string $query
    //  *
    //  * @return array
    //  */
    // public function bitmapSearch(string $query): array
    // {
    //     $table = $this->getTable();
    //     $result = $this->storage->getBitmap()->SEARCH($table, $query, 'WITHCURSOR');
    //     return [$result[0] ?? 0, $result[1] ?? null];
    // }

    // /**
    //  */
    // public function bitmapPopulate()
    // {
    //     $handleValues = $this->bitmap['handleValues'] ?? null;
    //     $generator = $this->bitmap === null ? [] : $this->iterate()->generator();
    //     $table = $this->getTable();
    //     $keys = array_keys($this->getBitmapFields());
    //     foreach ($generator as $id => $bean) {
    //         $values = $bean->getValues();
    //         if ($handleValues !== null) {
    //             $values = $handleValues($values);
    //         } else {
    //             $values = array_intersect_key($values, array_flip($keys));
    //         }
    //         $this->getBitmapBean($id, $values)->add();
    //     }
    // }

    

    // /**
    //  * @param array $scores
    //  *
    //  * @return array
    //  */
    // private function getSortChains(array $scores): array
    // {
    //     $ids1 = array_keys($scores);
    //     arsort($scores);
    //     $ids2 = array_keys($scores);
    //     $ids = array_combine($ids1, $ids2);
    //     $ids = array_filter($ids, function ($v, $k) {
    //         return $v != $k;
    //     }, ARRAY_FILTER_USE_BOTH);
    //     $chains = [];
    //     while ($ids) {
    //         $chain = [];
    //         reset($ids);
    //         $ex = key($ids);
    //         do {
    //             $chain[] = $ex;
    //             @ $_ = $ids[$ex];
    //             unset($ids[$ex]);
    //             @ $ex = $_;
    //         } while (isset($ids[$ex]));
    //         $chains[] = array_reverse($chain);
    //     }
    //     return $chains;
    // }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (substr($name, -4) === 'Sync') {
            $name = substr($name, 0, -4);
            return Promise\wait($this->$name(...$arguments));
        }
        $closure = function ($one, $two) {
            return function ($smth = null) use ($one, $two) {
                $filter = $this->config[$one][$two . 'PoolFilter'] ?? null;
                $pool = $one . 'Pool';
                $pool = $this->$pool;
                if ($filter === null) {
                    return $pool;
                }
                return $pool->filter($filter($smth, $pool->names()));
            };
        };
        $this->_map = $this->_map ?? [
            'hasDatabase' => function () {
                return $this->config['hasDatabase'];
            },
            'hasBitmap' => function () {
                return $this->config['hasBitmap'];
            },
            'getDatabaseTable' => function () {
                return $this->config['database']['table'];
            },
            'getBitmapIndex' => function () {
                return $this->config['bitmap']['index'];
            },
            //
            'getDatabaseFields' => function () {
                $fields = [];
                foreach ($this->config['database']['fields'] as $name => $field) {
                    $fields[$name] = new Field($name, $field['type']);
                }
                return $fields;
            },
            'getBitmapFields' => function () {
                $fields = [];
                foreach ($this->config['bitmap']['fields'] as $name => $field) {
                    $fields[$name] = new Field($name, $field['type']);
                }
                return $fields;
            },
            //
            'getDatabasePk' => function () {
                return $this->config['database']['pk'];
            },
            'getDatabasePkIncrementBy' => function ($name) {
                $get = $this->config['database']['getPkIncrementBy'] ?? null;
                if ($get === null) {
                    return 1;
                }
                return (int) $get($name, $this->databasePool->names());
            },
            'getDatabasePkStartWith' => function ($name) {
                $get = $this->config['database']['getPkStartWith'] ?? null;
                if ($get === null) {
                    return 1;
                }
                return (int) $get($name, $this->databasePool->names());
            },
            'getDatabaseIndexes' => function () {
                return $this->config['database']['indexes'];
            },
            'getDatabaseForeignKeys' => function () {
                return $this->config['database']['foreignKeys'];
            },
            //
            'getPrimaryDatabasePool' => $closure('database', 'primary'),
            'getWritableDatabasePool' => $closure('database', 'writable'),
            'getReadableDatabasePool' => $closure('database', 'readable'),
            'getPrimaryBitmapPool' => $closure('bitmap', 'primary'),
            'getWritableBitmapPool' => $closure('bitmap', 'writable'),
            'getReadableBitmapPool' => $closure('bitmap', 'readable'),
            //
            'getDatabaseBean' => function ($id = null, $values = []) {
                return $this->getDatabaseBeanWithValues($id, $values);
            },
            'getDatabaseBeanWithValues' => function ($id, $values) {
                $bean = $this->getDatabaseBeanWithFields($id, $this->getDatabaseFields());
                $bean->setValues($values);
                return $bean;
            },
            'getDatabaseBeanWithFields' => function ($id, $fields) {
                $bean = $this->config['database']['bean'] ?? DatabaseBean::class;
                return new $bean($this, $id, $fields);
            },
            'getBitmapBean' => function ($id = null, $values = []) {
                return $this->getBitmapBeanWithValues($id, $values);
            },
            'getBitmapBeanWithValues' => function ($id, $values) {
                $bean = $this->getBitmapBeanWithFields($id, $this->getBitmapFields());
                $bean->setValues($values);
                return $bean;
            },
            'getBitmapBeanWithFields' => function ($id, $fields) {
                $bean = $this->config['bitmap']['bean'] ?? BitmapBean::class;
                return new $bean($this, $id, $fields);
            },
        ];
        $method = $this->_map[$name] ?? null;
        if ($method === null) {
            throw new RuntimeException(sprintf(self::METHOD_NOT_FOUND, $name));
        }
        return $method(...$arguments);
    }
    
    /**
     * @return Storage
     */
    public function getStorage(): Storage
    {
        return $this->storage;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
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
        return $this->databasePool;
    }

    /**
     * @return Pool
     */
    public function getBitmapPool(): Pool
    {
        return $this->bitmapPool;
    }
}
