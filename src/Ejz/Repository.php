<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use RuntimeException;
use Ejz\Type\AbstractType;
use Closure;

class Repository implements NameInterface, ContextInterface
{
    use NameTrait;
    use SyncTrait;
    use ContextTrait;

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
            $index = $this->getBitmapIndex();
            $fields = $this->getBitmapFields();
            $fields[] = new Field('_names', Type::bitmapArray());
            $this->getMasterBitmapPool()->create($index, $fields);
        });
    }

    /**
     * @return Promise
     */
    public function createDatabase(): Promise
    {
        return \Amp\call(function () {
            $table = $this->getDatabaseTable();
            $pk = $this->getDatabasePk();
            $fields = $this->getDatabaseFields();
            $slaveFields = array_filter($fields, function ($field) {
                return $field->isSlave();
            });
            $indexes = $this->getDatabaseIndexes();
            $fks = $this->getDatabaseFks();
            $args = [
                $table, $pk,
                3 => 0, 4 => 0,
                5 => [], 6 => [],
                $fks,
            ];
            $pools = [$this->getMasterDatabasePool(), $this->getSlaveDatabasePool()];
            foreach ($pools as $i => $pool) {
                $isMaster = $i === 0;
                $names = $pool->names();
                $pkIncrement = count($names);
                $args[4] = $pkIncrement;
                yield $pool->each(function ($db) use (
                    $names, $args, $isMaster, $indexes, $fields, $slaveFields
                ) {
                    $pkStart = array_search($db->getName(), $names) + 1;
                    $args[3] = $pkStart;
                    $args[5] = $isMaster ? $fields : $slaveFields;
                    $args[6] = $isMaster ? $indexes : [];
                    return $db->create(...$args);
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
            $pk = $this->getDatabasePk();
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
                $fields = $isMaster ? $bean->getFields() : $bean->getSlaveFields();
                $promises += $pool->insert($table, $pk, $id, $fields);
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
     *
     * @return Iterator
     */
    public function get(array $ids): Iterator
    {
        $emit = function ($emit) use ($ids) {
            $ids = array_map('intval', $ids);
            $ids = array_values($ids);
            $table = $this->getDatabaseTable();
            $pools = [];
            $_ids = array_flip($ids);
            $meta = [];
            $cached = [];
            $cache = null;
            $cacheConfig = $this->getCacheConfig();
            if ($cacheConfig !== null) {
                $name = $this->getName();
                $keys = array_map(function ($id) use ($name) {
                    return $name . '.' . $id;
                }, $ids);
                $key2id = array_combine($keys, $ids);
                $cache = $this->getCache();
                $cached = $cache->getMultiple($keys);
                $cached = array_filter($cached, function ($value, $key) use (&$meta, $key2id, $name) {
                    if ($value !== null) {
                        return true;
                    }
                    $meta[$key2id[$key]] = [$key, [$name, $key]];
                    return false;
                }, ARRAY_FILTER_USE_BOTH);
            }
            foreach ($ids as $id) {
                if ($id <= 0) {
                    continue;
                }
                $pool = $this->getMasterDatabasePool($id);
                $names = $pool->names();
                if (!$names) {
                    continue;
                }
                sort($names, SORT_STRING);
                $name = implode(',', $names);
                $pools[$name] = $pools[$name] ?? ['pool' => $pool, 'ids' => []];
                $pools[$name]['ids'][] = $id;
            }
            $map = function ($value) use ($cacheConfig, $meta, $cache) {
                [$id] = $value;
                [$id, $bean] = $this->toDatabaseBean($value);
                if (isset($meta[$id])) {
                    [$ck, $ct] = $meta[$id];
                    $set = [$ck => $value];
                    foreach ($cacheConfig['cacheableFields'] as $field) {
                        $v = $bean->$field;
                        if ($v === null) {
                            continue;
                        }
                        $where = new WhereCondition([$field => $v]);
                        $set[md5(serialize($where->stringify()))] = $id;
                    }
                    $cache->setMultiple($set, $cacheConfig['ttl'], $ct);
                }
                return [$id, $bean];
            };
            $iterators = [Iterator::map(new Iterator($cached), $map)];
            foreach ($pools as $pool) {
                $params = $params ?? [
                    'pk' => [$this->getDatabasePk()],
                    'fields' => array_values($this->getDatabaseFields()),
                    'returnFields' => true,
                ];
                $iterator = $pool['pool']->random()->get($table, $pool['ids'], $params);
                $iterators[] = Iterator::map($iterator, $map);
            }
            $iterator = Iterator::merge($iterators, function ($value1, $value2) use ($_ids) {
                return $_ids[$value1[0]] - $_ids[$value2[0]];
            });
            $already = [];
            foreach ($ids as $id) {
                if (isset($already[$id])) {
                    yield $emit($already[$id]);
                    continue;
                }
                $value = null;
                if ($iterator !== null) {
                    $advance = yield $iterator->advance();
                    $value = $advance ? $iterator->getCurrent() : null;
                    $iterator = $value !== null ? $iterator : null;
                }
                if ($value === null) {
                    yield $emit([$id, null]);
                    continue;
                }
                [$vid] = $value;
                $already[$vid] = $value;
                if ($vid === $id) {
                    yield $emit($value);
                    continue;
                }
                yield $emit([$id, null]);
            }
        };
        return new Iterator($emit);
    }

    /**
     * @param array $value
     *
     * @return array
     */
    public function toDatabaseBean(array $value): array
    {
        [$id, $fields] = $value;
        $bean = $this->getDatabaseBeanWithFields($id, $fields);
        return [$id, $bean];
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
                $field = new Field('_names', Type::bitmapArray());
                $field->setValue($this->getMasterDatabasePool($id)->names());
                $fields[] = $field;
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
            $pk = $this->getDatabasePk();
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
                    $n += yield $db->delete($table, $pk, $ids);
                    $this->onDelete($ids);
                    continue;
                }
                $_ = $isMaster ? $fields : ($slaveFields ?? null);
                $_ = $_ ?? ($slaveFields = array_filter($fields, function ($field) {
                    return $field->isSlave();
                }));
                $n += yield $db->update($table, $pk, $ids, $_);
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
        foreach (array_chunk($ids, 1000) as $chunk) {
            $cache->drop(...array_map(function ($id) use ($name) {
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
                'pk' => [$this->getDatabasePk()],
                'fields' => array_values($this->getDatabaseFields()),
                'returnFields' => true,
            ] + $params + [
                'asc' => true,
            ];
            ['asc' => $asc] = $params;
            $pool = $this->getReadableMasterDatabasePool();
            $table = $this->getDatabaseTable();
            $iterators = $pool->each(function ($database) use ($table, $params) {
                $iterator = $database->iterate($table, $params);
                $iterator = Iterator::map($iterator, [$this, 'toDatabaseBean']);
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
     *
     * @return Iterator
     */
    public function filter($conditions): Iterator
    {
        $where = is_array($conditions) ? new WhereCondition($conditions) : $conditions;
        $cacheConfig = $this->getCacheConfig();
        if ($cacheConfig !== null && count($cacheConfig['cacheableFields'])) {
            $ck = md5(serialize($where->stringify()));
            $id = $this->getCache()->get($ck);
        }
        return isset($id) ? $this->get([$id]) : $this->iterate(compact('where'));
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
     * @param int $id1
     * @param int $id2
     *
     * @return Promise
     */
    public function reid(int $id1, int $id2): Promise
    {
        return \Amp\call(function ($id1, $id2) {
            $pool1 = $this->getWritableDatabasePool($id1);
            $pool2 = $this->getWritableDatabasePool($id2);
            $names1 = $pool1->names();
            $names2 = $pool2->names();
            if ($names1 !== $names2) {
                return false;
            }
            $table = $this->getDatabaseTable();
            $pk = $this->getDatabasePk();
            yield $pool1->reid($table, $pk, $id1, $id2);
            $this->dropCache([$id1]);
            return true;
        }, $id1, $id2);
    }

    /**
     * @return Promise
     */
    public function sort(): Promise
    {
        return \Amp\call(function () {
            $pk = $this->getDatabasePk();
            $div = mt_rand(1, 1);
            foreach (range(0, $div - 1) as $mod) {
                $scores = [];
                $iterator = $this->filter([[
                    $pk,
                    $mod,
                    '=',
                    '%s %% ' . $div,
                ]]);
                foreach ($iterator as $id => $bean) {
                    $scores[$id] = $this->getSortScore($bean);
                }
                foreach ($this->getSortChains($scores) as $ids) {
                    $max = max($ids);
                    $ids[] = -$max;
                    array_unshift($ids, -$max);
                    for ($i = count($ids); $i > 1; $i--) {
                        [$id1, $id2] = [$ids[$i - 2], $ids[$i - 1]];
                        if (!$this->reidSync($id1, $id2)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        });
    }
        
    /**
     */
    public function populateBitmap(): Promise
    {
        return \Amp\call(function () {
            yield $this->dropBitmap();
            yield $this->createBitmap();
            $iterator = $this->iterate();
            while ($iterator->advanceSync()) {
                [, $bean] = $iterator->getCurrent();
                $this->toBitmapBean($bean)->addSync();
            }
        });
    }

    /**
     * @param mixed $query
     * @param array $params (optional)
     *
     * @return Iterator
     */
    public function search($query, array $params = []): Iterator
    {
        $id = AbstractBean::ID;
        $params += [
            'sortby' => null,
            'asc' => true,
            'fks' => [],
        ];
        [
            'sortby' => $sortby,
            'asc' => $asc,
            'fks' => $fks,
        ] = $params;
        $index = $this->getBitmapIndex();
        $bitmap = $this->getReadableMasterBitmapPool()->random();
        $names = [null];
        if ($sortby === null && $this->hasSortScore()) {
            $names = $this->getReadableMasterDatabasePool()->names();
        }
        $size = 0;
        $iterators = array_map(function ($name) use (
            $bitmap, $index, $query, $params, &$size
        ) {
            if ($name !== null) {
                $query = "({$query}) & (@_names:{$name})";
            }
            $params = compact('query') + $params;
            $iterator = $bitmap->search($index, $params);
            $size += $iterator->getContext()['size'];
            $iterator = Iterator::chunk($iterator, 30);
            $repositories = [$this];
            $fks = $params['fks'];
            foreach ((array) $fks as $fk) {
                $repositories[] = $this->getRepositoryForFk($fk);
            }
            $emit = function ($emit) use ($iterator, $repositories) {
                while (yield $iterator->advance()) {
                    $chunk = $iterator->getCurrent();
                    $ids = [];
                    foreach ($chunk as [$id, $fks]) {
                        @ $ids[0][] = $id;
                        $len = $len ?? count($fks);
                        for ($i = 0; $i < $len; $i++) {
                            @ $ids[$i + 1][] = $fks[$i];
                        }
                    }
                    $iterators = [];
                    foreach ($ids as $idx => $_ids) {
                        $iterators[] = $repositories[$idx]->get($_ids);
                    }
                    $paired = Iterator::pair($iterators);
                    while (yield $paired->advance()) {
                        $value = $paired->getCurrent();
                        $value = array_merge(...$value);
                        yield $emit($value);
                    }
                }
            };
            return new Iterator($emit);
        }, array_combine($names, $names));
        $iterator = Iterator::merge($iterators, $this->getSortScoreClosure($asc));
        $iterator->setContext($size, 'size');
        return $iterator;
    }

    /**
     */
    private function normalize()
    {
        $cluster = 'm:!*;ms:1:id;s:!*;ss:1:id;';
        $this->config['database'] = $this->config['database'] ?? [];
        $this->config['bitmap'] = $this->config['bitmap'] ?? [];
        $this->config['database']['cluster'] = $this->config['database']['cluster'] ?? 'w:!*;';
        $this->config['bitmap']['cluster'] = $this->config['bitmap']['cluster'] ?? 'w:!*;';
        $this->config['database']['cluster'] = $cluster . $this->config['database']['cluster'];
        $this->config['bitmap']['cluster'] = $cluster . $this->config['bitmap']['cluster'];
        //
        $name = strtolower($this->getName());
        $this->config['database']['table'] = $this->config['database']['table'] ?? $name;
        $this->config['bitmap']['index'] = $this->config['bitmap']['index'] ?? $name;
        //
        $_ = $this->config['database']['pk'] ?? '%s_id';
        $this->config['database']['pk'] = sprintf($_, $this->config['database']['table']);
        //
        $fields = $this->config['database']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $field['slave'] = $field['slave'] ?? false;
            $collect[$name] = $field;
        }
        $this->config['database']['fields'] = $collect;
        //
        $indexes = $this->config['database']['indexes'] ?? [];
        $collect = [];
        foreach ($indexes as $name => $index) {
            $is_assoc = count($index) === count(array_filter(array_keys($index), 'is_string'));
            if (!$is_assoc) {
                $index = ['fields' => $index];
            }
            $type = $index['type'] ?? null;
            $collect[] = new Index($name, $index['fields'], $type);
        }
        $this->config['database']['indexes'] = $collect;
        //
        $fks = $this->config['database']['fks'] ?? [];
        $collect = [];
        foreach ($fks as $name => $fk) {
            if (is_string($fk)) {
                [$t, $f] = explode('.', $fk);
                $fk = [
                    'childFields' => explode(',', $f),
                    'parentFields' => explode(',', $f),
                    'parentTable' => $t,
                ];
            }
            $collect[] = new ForeignKey(
                $name,
                (array) $fk['childFields'],
                $fk['parentTable'],
                (array) $fk['parentFields']
            );
        }
        $this->config['database']['fks'] = $collect;
        //
        $fields = $this->config['bitmap']['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $field['slave'] = $field['slave'] ?? false;
            $collect[$name] = $field;
        }
        $this->config['bitmap']['fields'] = $collect;
        //
        $this->config['database']['bean'] = $this->config['database']['bean'] ?? DatabaseBean::class;
        $this->config['bitmap']['bean'] = $this->config['bitmap']['bean'] ?? BitmapBean::class;
        //
        $getSortScore = $this->config['getSortScore'] ?? null;
        $this->config['hasSortScore'] = $_ = is_callable($getSortScore);
        if (!$_) {
            $getSortScore = function () {
                return 0;
            };
        }
        $this->config['getSortScore'] = $getSortScore;
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
     * @param bool $asc
     *
     * @return Closure
     */
    public function getSortScoreClosure(bool $asc): Closure
    {
        return function ($a, $b) use ($asc) {
            [$id1, $bean1] = $a;
            [$id2, $bean2] = $b;
            $score1 = $this->getSortScore($bean1);
            $score2 = $this->getSortScore($bean2);
            if (!$asc) {
                [$score1, $score2] = [$score2, $score1];
                [$id1, $id2] = [$id2, $id1];
            }
            return ($score2 - $score1) ?: ($id1 - $id2);
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
    public function getDatabasePk(): string
    {
        return $this->config['database']['pk'];
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
     * @return array
     */
    public function getDatabaseFks(): array
    {
        return $this->config['database']['fks'];
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
    public function getBitmapFields(): array
    {
        $fields = $this->config['bitmap']['fields'];
        $collect = [];
        foreach ($fields as $name => $field) {
            $collect[$name] = new Field($name, $field['type']);
            $collect[$name]->isSlave($field['slave']);
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
     * @param string $fk
     *
     * @return self
     */
    public function getRepositoryForFk(string $fk): self
    {
        $fields = $this->getBitmapFields();
        foreach ($fields as $field) {
            if (!$field->getType()->is(Type::bitmapForeignKey())) {
                continue;
            }
            $parent = $field->getType()->getParentTable();
            return $this->getContext()['repositoryPool']->get($parent);
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
}
