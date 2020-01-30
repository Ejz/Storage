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

    /** @var array */
    protected $searchIteratorStates;

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
        $promises = $pool->add($index, $bean->id, $bean->getFields());
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
        $emitter = new \Amp\Emitter();
        $iterator = $emitter->iterate();
        $coroutine = \Amp\call(function ($ids) use (&$emitter) {
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
            if (count($iterators) > 1) {
                $iterator = Iterator\merge($iterators);
            } else {
                [$iterator] = $iterators;
            }
            while ((yield $iterator->advance()) && $emitter !== null) {
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
                yield $emitter->emit([$id, $bean]);
            }
        }, $ids);
        $coroutine->onResolve(function ($exception) use (&$emitter) {
            if ($exception) {
                $emitter->fail($exception);
                $emitter = null;
            } else {
                $emitter->complete();
            }
        });
        return new Emitter($iterator);
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
     * @return Iterator
     */
    public function iterate(array $params = []): Iterator
    {
        $emit = function ($emit) use ($params) {
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
            $iterator = Iterator\merge($iterators);
            $ids = [];
            while (yield $iterator->advance()) {
                [$id, $fields] = $iterator->getCurrent();
                if (isset($ids[$id])) {
                    continue;
                }
                $ids[$id] = true;
                $bean = $this->getDatabaseBeanWithFields($id, $fields);
                yield $emit([$id, $bean]);
            }
        };
        return new Producer($emit);
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
        $this->searchIteratorStates = [];
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

    /**
     * @param array $conditions
     *
     * @return Iterator
     */
    public function filter(array $conditions): Iterator
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
        return $this->filter($conditions)->advance();
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
     * @return Iterator
     */
    public function search($query): Iterator
    {
        $pool = $this->getReadableBitmapPool();
        $size = 0;
        $cursors = [];
        $iterators = $pool->each(function ($instance) use ($query, &$size, &$cursors) {
            $query = is_string($query) ? $query : ($query[$instance->getName()] ?? []);
            $iterator = $instance->search($this, $query);
            $size += $iterator->getSize();
            $cursors[] = $iterator->getCursor();
            return $iterator;
        });
        $cursor = md5(implode('', $cursors));
        $iterator = $this->getSearchIterator($cursor, $iterators);
        $iterator->setSize($size);
        $iterator->setCursor($cursor);
        $iterator->setContext(['repository' => $this, 'iterators' => $iterators]);
        return $iterator;
    }

    /**
     * @param array  $iterators
     * @param string $cursor
     *
     * @return Iterator
     */
    public function getSearchIterator(string $cursor, array $iterators): Iterator
    {
        $emitter = new \Amp\Emitter();
        $iterator = $emitter->iterate();
        $iterator = new Emitter($iterator);
        $coroutine = \Amp\call(function ($cursor, $iterators) use (&$emitter) {
            $score = [$this, 'getSortScore'];
            $values = [];
            $ids = [];
            $this->setSearchIteratorState($cursor, []);
            $iterator = Iterator\merge($iterators);
            while ((yield $iterator->advance()) && $emitter !== null) {
                yield $emitter->emit($iterator->getCurrent());
                // yield $emit($iterator->getCurrent());
            }
            return;
            while (true) {
                // print_r(array_keys(array_diff_key($iterators, $values)));
                $results = yield array_map(function ($iterator) {
                    return $iterator->advance();
                }, array_diff_key($iterators, $values));
                // var_dump($results);
                // var_dump($iterators['TEST0']->getSearchIteratorState()['pointer']);
                // var_dump($iterators['TEST1']->getSearchIteratorState()['pointer']);
                // var_dump($iterators['TEST2']->getSearchIteratorState()['pointer']);
                foreach ($results as $key => $result) {
                    if ($result) {
                        $value = $iterators[$key]->getCurrent();
                        $values[$key] = $value;
                        $ids[$key] = $value[0];
                    } else {
                        unset($iterators[$key]);
                    }
                }
                if (!$values) {
                    break;
                }
                uasort($values, function ($v1, $v2) use ($score) {
                    $s1 = $score($v1[1]);
                    $s2 = $score($v2[1]);
                    if (!$s1 && !$s2) {
                        return $v1[0] > $v2[0];
                    }
                    return $s1 < $s2;
                });
                $key = key($values);
                yield $emit($values[$key]);
                unset($values[$key], $ids[$key]);
                $this->setSearchIteratorState($cursor, $ids);
            }
        }, $cursor, $iterators);
        $coroutine->onResolve(function ($exception) use (&$emitter) {
            if ($exception) {
                $emitter->fail($exception);
                $emitter = null;
            } else {
                $emitter->complete();
            }
        });
        return $iterator;
    }

    public function setSearchIteratorState($cursor, $state)
    {
        $this->searchIteratorStates[$cursor] = $state;
    }

    public function getSearchIteratorState($cursor)
    {
        return $this->searchIteratorStates[$cursor];
    }

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
            'getSortScore' => function ($bean) {
                $getSortScore = $this->config['database']['getSortScore'] ?? null;
                return $getSortScore !== null ? $getSortScore($bean) : 0;
            },
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
            'getBitmapBean' => function ($id, $values = []) {
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
            'getBitmapBeanFromDatabaseBean' => function ($bean) {
                $id = (int) $bean->id;
                $getValues = $this->config['bitmap']['getValues'] ?? null;
                if ($getValues === null) {
                    $keys = array_keys($this->config['bitmap']['fields']);
                    $values = array_intersect_key($bean->getValues(), array_flip($keys));
                } else {
                    $values = $getValues($bean);
                }
                return $this->getBitmapBeanWithValues($id, $values);
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
