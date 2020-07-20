<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use RuntimeException;
use Closure;

class Repository implements NameInterface, ContextInterface
{
    use NameTrait;
    use ContextTrait;
    use SyncTrait;

    /** @var array */
    protected $config;

    /** @var DatabasePool */
    protected $databasePool;

    /** @var BitmapPool */
    protected $bitmapPool;

    /** @var RedisCache */
    protected $cache;

    /** @var array */
    protected $cached;

    /** @var string */
    private const BITMAP_SYSTEM_FIELD = '_names';

    /**
     * @param string          $name
     * @param array           $config
     * @param DatabasePool    $databasePool
     * @param BitmapPool      $bitmapPool
     * @param RedisCache      $cache
     */
    public function __construct(
        string $name,
        array $config,
        DatabasePool $databasePool,
        BitmapPool $bitmapPool,
        RedisCache $cache
    ) {
        $this->setName($name);
        $this->config = $config;
        $this->databasePool = $databasePool;
        $this->bitmapPool = $bitmapPool;
        $this->cache = $cache;
        $this->normalize();
    }

    /**
     * @return Promise
     */
    public function create(): Promise
    {
        return \Amp\call(function () {
            yield $this->createDatabase();
            yield $this->createBitmap();
        });
    }

    /**
     * @return Promise
     */
    public function createBitmap(): Promise
    {
        return \Amp\call(function () {
            yield $this->dropBitmap();
            $index = $this->getBitmapIndex();
            $fields = $this->getBitmapFields();
            if ($this->hasSortScore()) {
                $fields[] = new Field(self::BITMAP_SYSTEM_FIELD, BitmapType::ARRAY());
            }
            $this->getMasterBitmapPool()->create($index, $fields);
        });
    }

    /**
     * @return Promise
     */
    public function createDatabase(): Promise
    {
        return \Amp\call(function () {
            yield $this->dropDatabase();
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            $fields = $this->getDatabaseFields();
            $indexes = $this->getDatabaseIndexes();
            $foreignKeys = $this->getDatabaseForeignKeys();
            $args = [
                'table' => $table,
                'primaryKey' => $primaryKey,
                'primaryKeyStart' => 0,
                'primaryKeyIncrement' => 0,
                'fields' => $fields,
                'indexes' => $indexes,
                'foreignKeys' => $foreignKeys,
            ];
            $pools = [$this->getMasterDatabasePool(), $this->getSlaveDatabasePool()];
            $isSlave = function ($smth) {
                return is_object($smth) ? $smth->isSlave() : !empty($smth['slave']);
            };
            foreach ($pools as $i => $pool) {
                $isMaster = $i === 0;
                $names = $pool->names();
                $args['primaryKeyIncrement'] = count($names);
                yield $pool->each(function ($database) use ($names, $args, $isMaster, $isSlave) {
                    $args['primaryKeyStart'] = array_search($database->getName(), $names) + 1;
                    if (!$isMaster) {
                        $args['fields'] = array_filter($args['fields'], $isSlave);
                        $args['indexes'] = array_filter($args['indexes'], $isSlave);
                        $args['foreignKeys'] = array_filter($args['foreignKeys'], $isSlave);
                    }
                    return $database->create(...array_values($args));
                });
                $queries = $this->getDatabaseAdditionalCreateQueries();
                foreach ($queries as $query) {
                    yield $pool->exec($query);
                }
            }
        });
    }

    /**
     * @return Promise
     */
    public function drop(): Promise
    {
        return \Amp\call(function () {
            yield $this->dropDatabase();
            yield $this->dropBitmap();
        });
    }

    /**
     * @return Promise
     */
    public function dropDatabase(): Promise
    {
        return \Amp\call(function () {
            $table = $this->getDatabaseTable();
            yield $this->databasePool->drop($table);
        });
    }

    /**
     * @return Promise
     */
    public function dropBitmap(): Promise
    {
        return \Amp\call(function () {
            $index = $this->getBitmapIndex();
            yield $this->bitmapPool->drop($index);
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
        return \Amp\call(function ($bean) {
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            $master = $this->getMasterDatabasePool($bean);
            $names = $master->names();
            if (!$names) {
                return 0;
            }
            $id = yield $master->get($names[0])->getNextId($table);
            if ($id <= 0) {
                return 0;
            }
            $slave = $this->getSlaveDatabasePool($id);
            $promises = [];
            foreach ([$master, $slave] as $i => $pool) {
                $isMaster = $i === 0;
                $fields = $bean->getFields();
                if (!$isMaster) {
                    $fields = array_filter($fields, function ($field) {
                        return $field->isSlave();
                    });
                }
                $promises += $pool->insert($table, $primaryKey, $id, $fields);
            }
            [$errors] = yield Promise\any($promises);
            if ($errors) {
                return 0;
            }
            $this->onInsert($id);
            return $id;
        }, $bean);
    }

    /**
     * @param array $ids
     * @param array $params
     *
     * @return Iterator
     */
    public function get(array $ids, array $params = []): Iterator
    {
        $emit = function ($emit) use ($ids, $params) {
            $params += [
                'repeatable' => false,
                'nullable' => false,
            ];
            [
                'repeatable' => $repeatable,
                'nullable' => $nullable,
            ] = $params;
            $ids = array_map('intval', $ids);
            $ids = array_values($ids);
            $uniq = array_unique($ids);
            if (!$repeatable) {
                $ids = $uniq;
            }
            $_ids = array_flip($uniq);
            $table = $this->getDatabaseTable();
            $pools = [];
            $meta = [];
            $cached = [];
            $cache = null;
            $cacheConfig = $this->getCacheConfig();
            $alreadyCached = [];
            if ($cacheConfig !== null) {
                $name = $this->getName();
                $keys = array_map(function ($id) use ($name) {
                    return $name . '.' . $id;
                }, $ids);
                $key2id = array_combine($keys, $ids);
                $cache = $this->getCache();
                $cached = $cache->getMultiple($keys);
                $filter = function ($value, $key) use (&$meta, $key2id, $name, &$alreadyCached) {
                    if ($value !== null) {
                        $alreadyCached[$key2id[$key]] = true;
                        return true;
                    }
                    $meta[$key2id[$key]] = [$key, [$name, $key]];
                    return false;
                };
                $cached = array_filter($cached, $filter, ARRAY_FILTER_USE_BOTH);
            }
            foreach ($ids as $id) {
                if ($id < 1 || isset($alreadyCached[$id])) {
                    continue;
                }
                $pool = $this->getMasterDatabasePool($id);
                $names = $pool->names();
                $name = implode(',', $names);
                if (!$name) {
                    continue;
                }
                $pools[$name] = $pools[$name] ?? ['pool' => $pool, 'ids' => []];
                $pools[$name]['ids'][] = $id;
            }
            $setMultipleComplex = [[], [], [], []];
            $toDatabaseBean = function ($value) use ($cacheConfig, $meta, &$setMultipleComplex) {
                [$id, $values] = $value;
                $bean = $this->getDatabaseBeanWithValues($id, $values);
                if (isset($meta[$id])) {
                    [$ck, $ct] = $meta[$id];
                    $cl = [];
                    foreach ($cacheConfig['cacheableFields'] as $field) {
                        $v = $bean->$field;
                        $where = new WhereCondition([$field => $v]);
                        $cl[] = $where->key();
                    }
                    $setMultipleComplex[0][$ck] = $value;
                    $setMultipleComplex[1][$ck] = $cacheConfig['ttl'];
                    $setMultipleComplex[2][$ck] = $ct;
                    $setMultipleComplex[3][$ck] = $cl;
                }
                return $bean;
            };
            $iterators = [new Iterator($cached)];
            foreach ($pools as $pool) {
                $_params = $_params ?? [
                    'pk' => [$this->getDatabasePrimaryKey()],
                    'fields' => $this->getDatabaseFields(),
                ];
                $iterator = $pool['pool']->random()->get($table, $pool['ids'], $_params);
                $iterators[] = $iterator;
            }
            $iterator = Iterator::merge($iterators, function ($value1, $value2) use ($_ids) {
                return $_ids[$value1[0]] - $_ids[$value2[0]];
            });
            $collector = [];
            $pointer = 0;
            while (yield $iterator->advance()) {
                $collector[] = $toDatabaseBean($iterator->getCurrent());
            }
            if ($cache !== null) {
                $cache->setMultipleComplex(...$setMultipleComplex);
            }
            $already = [];
            foreach ($ids as $id) {
                if (isset($already[$id])) {
                    yield $emit($already[$id]);
                    continue;
                }
                $value = $collector[$pointer++] ?? null;
                if ($value === null) {
                    if ($nullable) {
                        yield $emit(null);
                    }
                    continue;
                }
                $vid = $value->id;
                $already[$vid] = $value;
                if ($vid === $id) {
                    yield $emit($already[$vid]);
                    continue;
                }
                if ($nullable) {
                    yield $emit(null);
                }
            }
        };
        return new Iterator($emit);
    }

    /**
     * @param DatabaseBean $bean
     *
     * @return BitmapBean
     */
    public function toBitmapBean(DatabaseBean $bean): BitmapBean
    {
        $id = (int) $bean->id;
        $toBitmapBean = $this->config['toBitmapBean'];
        $bean = $toBitmapBean($bean);
        if (!$bean instanceof BitmapBean) {
            $bean = $this->getBitmapBeanWithValues($id, $bean);
        }
        return $bean;
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
        return \Amp\call(function ($bean) {
            $index = $this->getBitmapIndex();
            $pool = $this->getMasterBitmapPool($bean);
            $fields = $bean->getFields();
            $id = $bean->id;
            if ($this->hasSortScore()) {
                $value = $this->getMasterDatabasePool($id)->names();
                $fields[] = new Field(self::BITMAP_SYSTEM_FIELD, BitmapType::ARRAY(), $value);
            }
            yield $pool->add($index, $id, $fields);
            return $id;
        }, $bean);
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
        return \Amp\call(function ($ids, $fields) {
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            $dbs = [];
            foreach ($ids as $id) {
                $master = $this->getMasterDatabasePool($id);
                $masterNames = $master->names();
                $slave = $this->getSlaveDatabasePool($id);
                $slaveNames = $slave->names();
                foreach (array_merge($masterNames, $slaveNames) as $name) {
                    $isMaster = array_search($name, $masterNames) !== false;
                    $dbs[$name] = $dbs[$name] ?? [
                        'db' => $isMaster ? $master->get($name) : $slave->get($name),
                        'ids' => [],
                        'isMaster' => $isMaster,
                    ];
                    $dbs[$name]['ids'][] = $id;
                }
            }
            $n = 0;
            foreach ($dbs as ['db' => $db, 'ids' => $ids, 'isMaster' => $isMaster]) {
                if ($fields === null) {
                    $n += yield $db->delete($table, $primaryKey, $ids);
                    $this->onDelete($ids);
                    continue;
                }
                $_ = $isMaster ? $fields : ($slaveFields ?? null);
                $_ = $_ ?? ($slaveFields = array_filter($fields, function ($field) {
                    return $field->isSlave();
                }));
                $n += yield $db->update($table, $primaryKey, $ids, $_);
                $this->onUpdate($ids);
            }
            return $n;
        }, $ids, $fields);
    }

    /**
     * @param array  $ids
     */
    public function onDelete(array $ids)
    {
        $this->dropCache($ids);
    }

    /**
     * @param array $ids
     */
    public function onUpdate(array $ids)
    {
        $this->dropCache($ids);
    }

    /**
     * @param int $id
     */
    public function onInsert(int $id)
    {
        $this->dropCache([$id]);
    }

    /**
     * @param array $ids
     */
    public function dropCache(array $ids)
    {
        $cache = $this->getCache();
        $name = $this->getName();
        foreach (array_chunk($ids, 1000, false) as $chunk) {
            $cache->dropUnion(...array_map(function ($id) use ($name) {
                return $name . '.' . $id;
            }, $chunk));
        }
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
                'pk' => [$this->getDatabasePrimaryKey()],
                'fields' => array_values($this->getDatabaseFields()),
            ] + $params + [
                'asc' => true,
                'pool' => null,
            ];
            ['asc' => $asc, 'pool' => $pool] = $params;
            $pool = $pool ?? $this->getReadableMasterDatabasePool();
            $table = $this->getDatabaseTable();
            $iterators = $pool->each(function ($database) use ($table, $params) {
                $iterator = $database->iterate($table, $params);
                $iterator = Iterator::map($iterator, function ($value) {
                    [$id, $values] = $value;
                    $bean = $this->getDatabaseBeanWithValues($id, $values);
                    return $bean;
                });
                return $iterator;
            });
            $iterator = Iterator::merge($iterators, $this->getSortScoreClosure($asc));
            while (yield $iterator->advance()) {
                yield $emit($iterator->getCurrent());
            }
        };
        return new Iterator($emit);
    }

    /**
     * @param mixed $conditions
     * @param array $params     (optional)
     *
     * @return Iterator
     */
    public function filter($conditions, array $params = []): Iterator
    {
        $where = is_array($conditions) ? new WhereCondition($conditions) : $conditions;
        $cacheConfig = $this->getCacheConfig();
        if (
            $cacheConfig !== null &&
            count($cacheConfig['cacheableFields']) &&
            count($where) === 1
        ) {
            $value = $this->getCache()->get($where->key());
            if ($value !== null) {
                [$id, $values] = $value;
                $bean = $this->getDatabaseBeanWithValues($id, $values);
                return new Iterator([$bean]);
            }
        }
        return $this->iterate(compact('where') + $params);
    }

    /**
     * @param mixed $conditions
     *
     * @return Promise
     */
    public function exists($conditions): Promise
    {
        return $this->filter($conditions)->advance();
    }

    /**
     * @param array $ids
     *
     * @return Promise
     */
    public function reid(array $ids): Promise
    {
        return \Amp\call(function ($ids) {
            $pools = array_map(function ($id) {
                return $this->getWritableDatabasePool($id);
            }, $ids);
            $ex = null;
            foreach ($pools as $pool) {
                $ex = $ex ?? $pool;
                if ($ex->names() !== $pool->names()) {
                    return false;
                }
                $ex = $pool;
            }
            if ($ex === null) {
                return false;
            }
            $table = $this->getDatabaseTable();
            $primaryKey = $this->getDatabasePrimaryKey();
            if (!yield $ex->reid($table, $primaryKey, $ids)) {
                return false;
            }
            $this->dropCache($ids);
            return true;
        }, $ids);
    }

    /**
     * @return Promise
     */
    public function sort(): Promise
    {
        return \Amp\call(function () {
            $primaryKey = $this->getDatabasePrimaryKey();
            $pool = $this->getReadableMasterDatabasePool();
            $names = $pool->names();
            $numbers = array_merge([1], $this->getPrimaryNumbers(1E4));
            $numbers = array_reverse($numbers);
            foreach ($numbers as $number) {
                $pattern = '$field %% ' . $number . ' $operation $value';
                $filters[] = [$primaryKey, 0, '=', $pattern];
            }
            foreach ($names as $name) {
                $_pool = $pool->filter($name);
                foreach ($filters as $filter) {
                    $iterator = $this->filter([$filter], ['pool' => $_pool]);
                    $scores = [];
                    [$min, $max] = [null, null];
                    foreach ($iterator as $bean) {
                        $score = $this->getSortScore($bean);
                        $scores[$bean->id] = $score;
                        $min = $min === null ? $score : min($score, $min);
                        $max = $max === null ? $score : max($score, $max);
                        if (count($scores) == 10000) {
                            break;
                        }
                    }
                    if (count($scores) < 2 || $min === $max) {
                        continue;
                    }
                    foreach ($this->getSortChains($scores) as $ids) {
                        if (!$this->reidSync($ids)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        });
    }

    /**
     * @return Promise
     */
    public function populateBitmap(): Promise
    {
        return \Amp\call(function () {
            yield $this->createBitmap();
            $iterator = $this->iterate();
            while (yield $iterator->advance()) {
                yield $this->toBitmapBean($iterator->getCurrent())->add();
            }
        });
    }

    /**
     * @param string $query
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function search(string $query, array $params = []): Iterator
    {
        $params += [
            'sortby' => null,
            'asc' => true,
            'min' => null,
            'max' => null,
            'foreignKeys' => [],
            'config' => [],
        ];
        [
            'sortby' => $sortby,
            'asc' => $asc,
            'min' => $min,
            'max' => $max,
            'foreignKeys' => $foreignKeys,
            'config' => $config,
        ] = $params;
        $config += $this->config;
        [
            'search_chunk_size' => $search_chunk_size,
        ] = $config;
        $foreignKeys = (array) $foreignKeys;
        $index = $this->getBitmapIndex();
        $bitmap = $this->getReadableMasterBitmapPool()->random();
        $names = [null];
        if ($this->hasSortScore()) {
            $names = $this->getReadableMasterDatabasePool()->names();
        }
        $args = [];
        $bitmap_system_field = self::BITMAP_SYSTEM_FIELD;
        foreach ($names as $name) {
            $q = $name === null ? $query : "({$query}) & (@{$bitmap_system_field}:{$name})";
            $args[] = [
                'query' => $q,
                'sortby' => $sortby,
                'asc' => $asc,
                'min' => $min,
                'max' => $max,
                'foreignKeys' => $foreignKeys,
                'forceForeignKeyFormat' => true,
                'emitTotal' => true,
                'config' => [
                    'iterator_chunk_size' => $search_chunk_size,
                ],
            ];
        }
        $iterators = $bitmap->search($index, $args);
        Promise\wait(Promise\all(array_map(function ($iterator) {
            return $iterator->advance();
        }, $iterators)));
        $sizes = array_map(function ($iterator) {
            return $iterator->getCurrent();
        }, $iterators);
        $size = array_sum($sizes);
        $iterators = array_map(function ($iterator) use ($search_chunk_size, $foreignKeys) {
            $iterator = Iterator::chunk($iterator, $search_chunk_size);
            $repositories = ['id' => $this];
            foreach ($foreignKeys as $foreignKey) {
                $repositories[$foreignKey] = $this->getRepositoryForForeignKey($foreignKey);
            }
            $emit = function ($emit) use ($iterator, $repositories) {
                while (yield $iterator->advance()) {
                    $rows = $iterator->getCurrent();
                    $iterators = [];
                    foreach ($repositories as $key => $repository) {
                        $ids = array_column($rows, $key);
                        $iterators[$key] = $repository->get($ids, [
                            'repeatable' => $key !== 'id',
                            'nullable' => $key !== 'id',
                        ]);
                    }
                    $paired = Iterator::pair($iterators);
                    while (yield $paired->advance()) {
                        $value = $paired->getCurrent();
                        $bean = $value['id'];
                        unset($value['id']);
                        foreach ($value as $k => $v) {
                            $bean->$k = $v ?? $bean->$k;
                        }
                        yield $emit($bean);
                    }
                }
            };
            return new Iterator($emit);
        }, $iterators);
        $iterator = Iterator::merge($iterators, $this->getSortScoreClosure($asc));
        $iterator->setContext($size, 'size');
        return $iterator;
    }

    /**
     */
    private function normalize()
    {
        $this->config = $this->config + [
            'database' => [],
            'bitmap' => [],
            'get_chunk_size' => 100,
            'search_chunk_size' => 10,
        ];
        $cluster = 'm:!*;ms:1:id;s:!*;ss:1:id;';
        $this->config['database']['cluster'] = $this->config['database']['cluster'] ?? 'w:!*;';
        $this->config['bitmap']['cluster'] = $this->config['bitmap']['cluster'] ?? 'w:!*;';
        $this->config['database']['cluster'] = $cluster . $this->config['database']['cluster'];
        $this->config['bitmap']['cluster'] = $cluster . $this->config['bitmap']['cluster'];
        //
        $name = strtolower($this->getName());
        $this->config['database']['table'] = $this->config['database']['table'] ?? $name;
        $this->config['bitmap']['index'] = $this->config['bitmap']['index'] ?? $name;
        //
        $_ = $this->config['database']['primaryKey'] ?? '%s_id';
        $this->config['database']['primaryKey'] = sprintf($_, $this->config['database']['table']);
        //
        $fields = $this->config['database']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if (!is_array($field)) {
                $field = ['type' => $field];
            }
            $field['slave'] = $field['slave'] ?? false;
            $collect[$name] = $field;
        }
        $this->config['database']['fields'] = $collect;
        //
        $this->config['database']['indexes'] = $this->config['database']['indexes'] ?? [];
        //
        $this->config['database']['foreignKeys'] = $this->config['database']['foreignKeys'] ?? [];
        //
        $this->config['database']['additional_create_queries'] = $this->config['database']['additional_create_queries'] ?? [];
        //
        $fields = $this->config['bitmap']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if (!is_array($field)) {
                $field = ['type' => $field];
            }
            $collect[$name] = $field;
        }
        $this->config['bitmap']['fields'] = $collect;
        //
        $this->config['database']['bean'] = $this->config['database']['bean'] ?? DatabaseBean::class;
        $this->config['bitmap']['bean'] = $this->config['bitmap']['bean'] ?? BitmapBean::class;
        //
        $getSortScore = $this->config['getSortScore'] ?? null;
        $this->config['hasSortScore'] = $_ = is_callable($getSortScore);
        $this->config['getSortScore'] = $_ ? $getSortScore : function () { return 0; };
        //
        $this->config['cache'] = $this->config['cache'] ?? null;
        if ($this->config['cache'] !== null) {
            $this->config['cache'] += [
                'ttl' => 3600,
                'cacheableFields' => [],
            ];
        }
        //
        $toBitmapBean = $this->config['toBitmapBean'] ?? null;
        if (!is_callable($toBitmapBean)) {
            $keys = array_flip(array_keys($this->config['bitmap']['fields']));
            $toBitmapBean = function (DatabaseBean $bean) use ($keys) {
                return array_intersect_key($bean->getValues(), $keys);
            };
        }
        $this->config['toBitmapBean'] = $toBitmapBean;
        //
        if (count($this->getSlaveBitmapPool()) > 0) {
            throw new RuntimeException();
        }
        if (count($this->getReadableMasterBitmapPool()) > 1) {
            throw new RuntimeException();
        }
    }

    /**
     * @param DatabaseBean $bean
     *
     * @return int
     */
    public function getSortScore(DatabaseBean $bean): int
    {
        $getSortScore = $this->config['getSortScore'];
        return $getSortScore($bean);
    }

    /**
     * @return bool
     */
    public function hasSortScore(): bool
    {
        return $this->config['hasSortScore'];
    }

    /**
     * @param ?bool $asc
     *
     * @return Closure
     */
    public function getSortScoreClosure(?bool $asc): Closure
    {
        return function ($bean1, $bean2) use ($asc) {
            if ($asc === null) {
                return mt_rand(0, 1) ? +1 : -1;
            }
            $score1 = $this->getSortScore($bean1);
            $score2 = $this->getSortScore($bean2);
            if (!$asc) {
                return ($score1 - $score2) ?: ($bean2->id - $bean1->id);
            }
            return ($score2 - $score1) ?: ($bean1->id - $bean2->id);
        };
    }

    /**
     * @return RedisCache
     */
    public function getCache(): RedisCache
    {
        return $this->cache;
    }

    /**
     * @return DatabasePool
     */
    public function getDatabasePool(): DatabasePool
    {
        return $this->databasePool;
    }
 
    /**
     * @param mixed $value (optional)
     *
     * @return DatabasePool
     */
    public function getWritableDatabasePool($value = null): DatabasePool
    {
        $master = $this->getMasterDatabasePool($value);
        $slave = $this->getSlaveDatabasePool($value);
        $names = array_merge($master->names(), $slave->names());
        return $this->databasePool->filter($names);
    }

    /**
     * @return DatabasePool
     */
    public function getReadableMasterDatabasePool(): DatabasePool
    {
        return $this->getReadablePool('MasterDatabase');
    }

    /**
     * @return DatabasePool
     */
    public function getReadableSlaveDatabasePool(): DatabasePool
    {
        return $this->getReadablePool('SlaveDatabase');
    }

    /**
     * @param mixed $value (optional)
     *
     * @return DatabasePool
     */
    public function getMasterDatabasePool($value = null): DatabasePool
    {
        return $this->getPool('database', 'm', $value);
    }

    /**
     * @param mixed $value (optional)
     *
     * @return DatabasePool
     */
    public function getSlaveDatabasePool($value = null): DatabasePool
    {
        return $this->getPool('database', 's', $value);
    }

    /**
     * @return BitmapPool
     */
    public function getBitmapPool(): BitmapPool
    {
        return $this->bitmapPool;
    }

    /**
     * @param mixed $value (optional)
     *
     * @return DatabasePool
     */
    public function getWritableBitmapPool($value = null): BitmapPool
    {
        $master = $this->getMasterBitmapPool($value);
        $slave = $this->getSlaveBitmapPool($value);
        $names = array_merge($master->names(), $slave->names());
        return $this->bitmapPool->filter($names);
    }

    /**
     * @return DatabasePool
     */
    public function getReadableMasterBitmapPool(): BitmapPool
    {
        return $this->getReadablePool('MasterBitmap');
    }

    /**
     * @return BitmapPool
     */
    public function getReadableSlaveBitmapPool(): BitmapPool
    {
        return $this->getReadablePool('SlaveBitmap');
    }

    /**
     * @param mixed $value (optional)
     *
     * @return BitmapPool
     */
    public function getMasterBitmapPool($value = null): BitmapPool
    {
        return $this->getPool('bitmap', 'm', $value);
    }

    /**
     * @param mixed $value (optional)
     *
     * @return BitmapPool
     */
    public function getSlaveBitmapPool($value = null): BitmapPool
    {
        return $this->getPool('bitmap', 's', $value);
    }

    /**
     * @param string $prefix
     * @param string $type
     * @param mixed  $value
     *
     * @return PoolInterface
     */
    private function getPool(string $prefix, string $type, $value): PoolInterface
    {
        $this->cached = $this->cached ?? [];
        if (!isset($this->cached[$prefix][$type])) {
            preg_match_all(
                '~(\w+):(.*?)(;|$)~',
                $this->config[$prefix]['cluster'],
                $matches,
                PREG_SET_ORDER
            );
            $collect = [];
            foreach ($matches as $match) {
                $collect[$match[1]] = $match[2];
            }
            $pool = $prefix . 'Pool';
            $pool = $this->$pool;
            $filter = self::cluster2filter($collect[$type]);
            $pool = $pool->filter($filter);
            if ($type === 's') {
                $exclude = $this->getPool($prefix, 'm', null)->names();
                $pool = $pool->filter(function ($name) use ($exclude) {
                    return !in_array($name, $exclude);
                });
            }
            $this->cached[$prefix][$type] = [
                'pool' => $pool,
                'ms' => $collect['ms'],
                'ss' => $collect['ss'],
            ];
        }
        ['pool' => $pool, 'ms' => $ms, 'ss' => $ss] = $this->cached[$prefix][$type];
        $names = $pool->names();
        $c = count($names);
        if ($value === null || $c === 0) {
            return $pool;
        }
        $s = $type . 's';
        [$n, $fields] = explode(':', $$s);
        $fields = explode(',', $fields);
        $n = $n === '*' ? $c : (int) $n;
        $n = min($c, $n);
        $n = max($n, 1);
        if (!is_object($value)) {
            $value = (object) ['id' => $value];
        }
        $seed = null;
        foreach ($fields as $field) {
            $v = $value->$field ?? null;
            if ($v !== null) {
                $seed = is_numeric($v) ? abs((int) $v) : crc32($v);
                break;
            }
        }
        $seed = $seed ?? mt_rand();
        $seed = ($seed % $c) - 1;
        $seed = $seed < 0 ? $seed + $c : $seed;
        $collect = [];
        for ($i = 0; $i < $n; $i++) {
            $collect[] = $pool->get($names[($seed + $i) % $c]);
        }
        $pool = $prefix === 'bitmap' ? BitmapPool::class : DatabasePool::class;
        return new $pool($collect);
    }

    /**
     * @param string $method
     *
     * @return PoolInterface
     */
    private function getReadablePool(string $method): PoolInterface
    {
        $method = "get{$method}Pool";
        $pool = $this->$method();
        $count = count($pool);
        $range = [];
        if ($count) {
            $n = count($this->$method(0));
            $rnd = mt_rand(0, $count - 1);
            $range = range($rnd, $rnd + $count - 1);
            $range = array_chunk($range, $n);
            $range = array_map(function ($vals) use ($count) {
                return $vals[array_rand($vals)] % $count;
            }, $range);
        }
        return $pool->filter(function ($name, $names) use ($range) {
            return in_array(array_search($name, $names), $range, true);
        });
    }

    /**
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @return string
     */
    public function getDatabaseTable(): string
    {
        return $this->config['database']['table'];
    }

    /**
     * @return string
     */
    public function getDatabasePrimaryKey(): string
    {
        return $this->config['database']['primaryKey'];
    }

    /**
     * @return array
     */
    public function getDatabaseFields(): array
    {
        $fields = $this->config['database']['fields'];
        $collect = [];
        foreach ($fields as $name => $field) {
            $collect[$name] = new Field($name, $field['type']);
            $collect[$name]->isSlave($field['slave']);
        }
        return $collect;
    }

    /**
     * @return array
     */
    public function getDatabaseIndexes(): array
    {
        return $this->config['database']['indexes'];
    }

    /**
     * @return string
     */
    public function getBitmapIndex(): string
    {
        return $this->config['bitmap']['index'];
    }

    /**
     * @return array
     */
    public function getDatabaseForeignKeys(): array
    {
        return $this->config['database']['foreignKeys'];
    }

    /**
     * @return array
     */
    public function getDatabaseAdditionalCreateQueries(): array
    {
        return $this->config['database']['additional_create_queries'];
    }

    /**
     * @return array
     */
    public function getBitmapFields(): array
    {
        $fields = $this->config['bitmap']['fields'];
        $collect = [];
        foreach ($fields as $name => $field) {
            $collect[$name] = new Field($name, $field['type']);
        }
        return $collect;
    }

    /**
     * @param ?int  $id     (optional)
     * @param array $values (optional)
     *
     * @return DatabaseBean
     */
    public function getDatabaseBean(?int $id = null, array $values = []): DatabaseBean
    {
        return $this->getDatabaseBeanWithValues($id, $values);
    }

    /**
     * @param ?int  $id
     * @param array $values
     *
     * @return DatabaseBean
     */
    public function getDatabaseBeanWithValues(?int $id, array $values): DatabaseBean
    {
        $bean = $this->getDatabaseBeanWithFields($id, $this->getDatabaseFields());
        $bean->setValues($values);
        return $bean;
    }

    /**
     * @param ?int  $id
     * @param array $fields
     *
     * @return DatabaseBean
     */
    public function getDatabaseBeanWithFields(?int $id, array $fields): DatabaseBean
    {
        $bean = $this->config['database']['bean'];
        return new $bean($this, $id, $fields);
    }

    /**
     * @param ?int  $id     (optional)
     * @param array $values (optional)
     *
     * @return BitmapBean
     */
    public function getBitmapBean(?int $id = null, array $values = []): BitmapBean
    {
        return $this->getBitmapBeanWithValues($id, $values);
    }

    /**
     * @param ?int  $id
     * @param array $values
     *
     * @return BitmapBean
     */
    public function getBitmapBeanWithValues(?int $id, array $values): BitmapBean
    {
        $bean = $this->getBitmapBeanWithFields($id, $this->getBitmapFields());
        $bean->setValues($values);
        return $bean;
    }

    /**
     * @param ?int  $id
     * @param array $fields
     *
     * @return BitmapBean
     */
    public function getBitmapBeanWithFields(?int $id, array $fields): BitmapBean
    {
        $bean = $this->config['bitmap']['bean'];
        return new $bean($this, $id, $fields);
    }

    /**
     * @return ?array
     */
    public function getCacheConfig(): ?array
    {
        return $this->config['cache'];
    }

    /**
     * @param string $foreignKey
     *
     * @return self
     */
    public function getRepositoryForForeignKey(string $foreignKey): self
    {
        $fields = $this->getBitmapFields();
        foreach ($fields as $field) {
            if ($field->getName() !== $foreignKey) {
                continue;
            }
            $type = $field->getType();
            if ($type->getName() !== 'FOREIGNKEY') {
                continue;
            }
            $options = $type->getOptions();
            $references = $options['references'];
            $repositoryPool = $this->getContext()['repositoryPool'];
            foreach ($repositoryPool->names() as $name) {
                $repository = $repositoryPool->get($name);
                if ($repository->getBitmapIndex() === $references) {
                    return $repository;
                }
            }
        }
    }

    /**
     * @param string $notation
     *
     * @return Closure
     */
    public static function cluster2filter(string $notation): Closure
    {
        return function ($name, $names) use ($notation) {
            $idx = array_search($name, $names);
            foreach (explode(',', $notation) as $n) {
                $neg = false;
                while (strpos($n, '!') === 0) {
                    $neg = !$neg;
                    $n = substr($n, 1);
                }
                $trig =
                    ($n === '*') ||
                    (is_numeric($n) && $n == $idx) ||
                    (!is_numeric($n) && $n === $name)
                ;
                if ($trig) {
                    return !$neg;
                }
                if  ($neg) {
                    return true;
                }
            }
            return false;
        };
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
     * @param int $max
     *
     * @return array
     */
    private function getPrimaryNumbers(int $max): array
    {
        $number = 1;
        $collect = [];
        while ($number++ < $max) {
            $ok = true;
            for ($i = 2; $i < $number; $i++) {
                if (($number % $i) == 0) {
                    $ok = false;
                    break;
                }
            }
            if ($ok) {
                $collect[] = $number;
            }
        }
        return $collect;
    }
}
