<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use RuntimeException;
use Ejz\Type\AbstractType;
use Closure;

class Repository implements NameInterface
{
    use NameTrait;
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

    /**
     * @param string  $name
     * @param Storage $storage
     * @param array   $config
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
            yield $this->databaseCreate();
            yield $this->bitmapCreate();
        });
    }

    /**
     * @return Promise
     */
    public function bitmapCreate(): Promise
    {
        return \Amp\call(function () {
            $index = $this->getBitmapIndex();
            $fields = $this->getBitmapFields();
            $slaveFields = array_filter($fields, function ($field) {
                return $field->isSlave();
            });
            $pools = [$this->getMasterBitmapPool(), $this->getSlaveBitmapPool()];
            foreach ($pools as $i => $pool) {
                $isMaster = $i === 0;
                yield $pool->create($index, $isMaster ? $fields : $slaveFields);
            }
        });
    }

    /**
     * @return Promise
     */
    public function databaseCreate(): Promise
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
            $table = $this->getDatabaseTable();
            yield $this->databasePool->drop($table);
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
            $pool = $this->getMasterDatabasePool($bean);
            $names = $pool->names();
            if (!$names) {
                return 0;
            }
            $id = yield $pool->get($names[0])->getNextId($table);
            if ($id <= 0) {
                return 0;
            }
            $pools = [$pool, $this->getSlaveDatabasePool()];
            foreach ($pools as $i => $pool) {
                $isMaster = $i === 0;
                $fields = $isMaster ? $bean->getFields() : $bean->getSlaveFields();
                yield $pool->insert($table, $pk, $id, $fields);
            }
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
            $table = $this->getDatabaseTable();
            $dbs = [];
            $_ids = array_flip($ids);
            $meta = [];
            $cached = [];
            $cache = null;
            $cacheConfig = $this->getCacheConfig();
            foreach ($ids as $id) {
                if ($cacheConfig !== null) {
                    $cache = $cache ?? $this->getCache();
                    $name = $name ?? $this->getName();
                    $ck = $name . '.' . $id;
                    $v = $cache->get($ck);
                    if ($v !== null) {
                        $cached[] = $v;
                        continue;
                    }
                    $ct = [$name, $ck];
                    $meta[$id] = [$ck, $ct];
                }
                $db = $this->getMasterDatabasePool($id)->random();
                $name = $db->getName();
                $dbs[$name] = $dbs[$name] ?? ['db' => $db, 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
            $map = function ($value) use ($cacheConfig, $meta, $cache) {
                [$id] = $value;
                $bean = $this->toDatabaseBean($value);
                if (isset($meta[$id])) {
                    [$ck, $ct] = $meta[$id];
                    $cache->set($ck, $value, $cacheConfig['ttl'], $ct);
                    foreach ($cacheConfig['cacheableFields'] as $field) {
                        $v = $bean->$field;
                        if ($v === null) {
                            continue;
                        }
                        $where = new WhereCondition([$field => $v]);
                        $ck = md5(serialize($where->stringify()));
                        $cache->set($ck, $id, $cacheConfig['ttl'], $ct);
                    }
                }
                return $bean;
            };
            $iterators = [Iterator::map(new Iterator($cached), $map)];
            foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
                $params = $params ?? [
                    'pk' => [$this->getDatabasePk()],
                    'fields' => array_values($this->getDatabaseFields()),
                    'returnFields' => true,
                ];
                $iterator = $db->get($table, $ids, $params);
                $iterators[] = Iterator::map($iterator, $map);
            }
            $iterator = Iterator::merge($iterators, function ($value1, $value2) use ($_ids) {
                return $_ids[$value1[0]] - $_ids[$value2[0]];
            });
            while (yield $iterator->advance()) {
                yield $emit($iterator->getCurrent());
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

    // /**
    //  * @param array $value
    //  *
    //  * @return array
    //  */
    // public function toDatabaseBean(array $value): array
    // {
    //     [$id, $fields] = $value;
    //     $_fields = [];
    //     foreach ($fields as $key => $field) {
    //         $_fields[$key] = clone $field;
    //     }
    //     $bean = $this->getDatabaseBeanWithFields($id, $_fields);
    //     return [$id, $bean];
    // }

            // while (($value = yield $iterator->pull()) !== null) {
            //     
            //     $bean = $this->getDatabaseBeanWithFields($id, $fields);
            //     if (isset($meta[$id])) {
            //         [$ck, $ct] = $meta[$id];
            //         $cache = $cache ?? $this->storage->getCache();
            //         $cache->set($ck, $value, $this->config['cache']['ttl'], $ct);
            //         foreach ($this->config['cache']['fieldsToId'] as $f2id) {
            //             $v = md5(serialize($bean->$f2id));
            //             $ck = $table . '.' . $f2id . '.' . $v;
            //             $cache->set($ck, $id, $this->config['cache']['ttl'], $ct);
            //         }
            //     }
            //     yield $emitter->push([$id, $bean]);
            // }
            
    // /**
    //  * @param int   $id
    //  * @param array $values (optional)
    //  *
    //  * @return Promise
    //  */
    // public function add(int $id, array $values = []): Promise
    // {
    //     $bean = $this->getBitmapBeanWithValues($id, $values);
    //     return $this->addBean($bean);
    // }

    // /**
    //  * @param BitmapBean $bean
    //  *
    //  * @return Promise
    //  */
    // public function addBean(BitmapBean $bean): Promise
    // {
    //     $deferred = new Deferred();
    //     $pool = $this->getWritableBitmapPool($bean->id);
    //     $index = $this->getBitmapIndex();
    //     // $shard = ->random()->getName();
    //     $_ = $this->getWritableDatabasePool($bean->id);
    //     $promises = $pool->add($index, $bean->id, $bean->getFields(), $_);
    //     Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
    //         $ids = $err ? [0] : ($res ?: [0]);
    //         $min = min($ids);
    //         $max = max($ids);
    //         $deferred->resolve($min === $max ? $min : null);
    //     });
    //     return $deferred->promise();
    // }

    

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
            ] + $params;
            $pool = $this->getReadableMasterDatabasePool();
            $table = $this->getDatabaseTable();
            $iterators = $pool->each(function ($db) use ($table, $params) {
                $iterator = $db->iterate($table, $params);
                $iterator = Iterator::map($iterator, [$this, 'toDatabaseBean']);
                return $iterator;
            });
            $iterator = Iterator::merge($iterators, function ($a, $b) {
                [$id1, $bean1] = $a;
                [$id2, $bean2] = $b;
                $score1 = $this->getSortScore($bean1);
                $score2 = $this->getSortScore($bean2);
                return ($score2 - $score1) ?: ($id1 - $id2);
            });
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
        

    

    

    

    

    // /**
    //  */
    // public function bitmapPopulate()
    // {
    //     $this->bitmapPool->dropSync($this->getBitmapIndex());
    //     $this->bitmapPool->createSync($this);
    //     foreach ($this->iterate()->generator() as $bean) {
    //         $this->getBitmapBeanFromDatabaseBean($bean)->addSync();
    //     }
    // }

    // /**
    //  * @param string|array $query
    //  *
    //  * @return Emitter
    //  */
    // public function search($query): Emitter
    // {
    //     $db = $this->getReadableDatabasePool();
    //     $pool = $this->getReadableBitmapPool();
    //     $bitmap = $pool->random();
    //     $iterators = [];
    //     $size = 0;
    //     foreach ($db->names() as $name) {
    //         if (is_string($query)) {
    //             $q = "({$query}) @_shard:{$name}";
    //         } else {
    //             $q = $query[$name];
    //         }
    //         $iterator = $bitmap->search($this, $q);
    //         $size += $iterator->getSize();
    //         $iterators[$name] = $iterator;
    //     }
    //     $iterator = $this->getSearchIterator($iterators);
    //     $iterator->setSize($size);
    //     return $iterator;
    // }

    // // *
    // //  * @param array  $iterators
    // //  * @param string $cursor
    // //  *
    // //  * @return Emitter
     
    // // public function getSearchIterator(array $iterators): Emitter
    // // {
        
    // // }

    

    

    

    
    
    
    
    /**
     */
    private function normalize()
    {
        $this->config['aliases'] = $this->config['aliases'] ?? [];
        $this->config['aliases'] = array_map('strtolower', $this->config['aliases']);
        //
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
        $this->config['getSortScore'] = $this->config['getSortScore'] ?? null;
        if (!is_callable($this->config['getSortScore'])) {
            $this->config['getSortScore'] = null;
        }
        $this->config['cache'] = $this->config['cache'] ?? null;
        if ($this->config['cache'] !== null) {
            $this->config['cache'] += [
                'ttl' => 3600,
                'cacheableFields' => [],
            ];
        }
    }

    /**
     * @param AbstractBean $bean
     *
     * @return int
     */
    public function getSortScore(AbstractBean $bean): int
    {
        $getSortScore = $this->config['getSortScore'];
        return $getSortScore === null ? $bean->id : $getSortScore($bean);
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
        return new DatabasePool($collect);
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
// $deferred = new Deferred();
        // $pk = $this->getDatabasePk();
        // $fields = $bean->getFields();
        // $slaveFields = $bean->getSlaveFields();

        // $promises = [];
        // $pools = [];
        // foreach ([Pool::POOL_PRIMARY, Pool::POOL_SECONDARY] as $type) {
        //     $isSecondary = $type === Pool::POOL_SECONDARY;
        //     $pool = $this->getDatabasePool($type | Pool::POOL_RANDOM, null);
        //     $_ = $isSecondary ? [] : $fields;
        //     $promises[] = Promise\any($pool->insertNew($table, $primaryKey, $_));
        //     $pools[] = $pool;
        // }
        // $promise = Promise\all($promises);
        // $promise->onResolve(function ($e, $v) use ($deferred, $pools, $table, $primaryKey) {
        //     $id = null;
        //     $collect = [];
        //     foreach ($v as $idx => [$err, $ids]) {
        //         $collect[] = ['pool' => $pools[$idx], 'ids' => $ids];
        //         $min = $ids ? min($ids) : 0;
        //         $max = $ids ? max($ids) : 0;
        //         if ($err || !$min || !$max || $min !== $max) {
        //             $id = 0;
        //         }
        //         if ($id === 0) {
        //             continue;
        //         }
        //         if ($min !== $max) {
        //             $id = 0;
        //         }
        //     }
        //     foreach (($id ? [] : $collect) as ['pool' => $pool, 'ids' => $ids]) {
        //         $pool->deleteNewSync($table, $primaryKey, $ids);
        //     }
        //     $deferred->resolve($id);
        // });
        // return $deferred->promise();

        // $promise->onResolve(function ($e, $v) use ($deferred) {
        //     $deferred->resolve($min === $max ? $min : null);
        // });
        // return $promise;
        // //
        // $primaryPool = $this->getDatabasePool(Pool::POOL_PRIMARY, $bean);
        // $promises = $primaryPool->insertNew($table, $primaryKey, $fields);
        // $promise = Promise\all($promises);
        // //
        
        // $promise->onResolve(function ($err, $res) use ($deferred, $primaryPool) {
        //     $ids = $err ? [0] : ($res ?: [0]);
        //     $min = min($ids);
        //     $max = max($ids);
        //     $deferred->resolve($min === $max ? $min : null);
        // });

        // $names = $primaryPool->names();
        // $promises = $primaryPool->each(function ($db) use ($names, $args) {
        //     $primaryKeyStart = array_search($db->getName(), $names) + 1;
        //     $primaryKeyIncrement = count($names);
        //     [$args[3], $args[4]] = [$primaryKeyStart, $primaryKeyIncrement];
        //     return $db->createNew(...$args);
        // });
        // yield $promises;
        // //
        // $secondaryPool = $this->getDatabasePool(Pool::POOL_SECONDARY);
        // $names = $secondaryPool->names();
        // $promises = $secondaryPool->each(function ($db) use ($names, $args) {
        //     $primaryKeyStart = array_search($db->getName(), $names) + 1;
        //     $primaryKeyIncrement = count($names);
        //     [$args[3], $args[4]] = [$primaryKeyStart, $primaryKeyIncrement];
        //     [$args[5], $args[6]] = [[], []];
        //     return $db->createNew(...$args);
        // });
        // yield $promises;

        // return \Amp\call(function () {
            
        // });
        // if (!empty($this->config['database']['cluster'])) {
            
        // } else {
        //     $pool = $this->getWritableDatabasePool($bean);
        //     $promises = $pool->insert($this, $bean->getFields());
        //     Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
        //         $ids = $err ? [0] : ($res ?: [0]);
        //         $min = min($ids);
        //         $max = max($ids);
        //         $deferred->resolve($min === $max ? $min : null);
        //     });
        // }
        // return $deferred->promise();// $is_assoc = function ($array) {
        //     if (!is_array($array)) {
        //         return false;
        //     }
        //     $count0 = count($array);
        //     $count1 = ;
        //     return $count0 === $count1;
        // };
        //
        //
        //
        // $this->config['hasDatabase'] = isset($this->config['database']);
        // $this->config['hasBitmap'] = isset($this->config['bitmap']);
        // //
        // //
        // //
        // $this->config['database']['cluster'] = $this->config['database']['cluster'] ?? '';
        // $this->config['bitmap']['cluster'] = $this->config['bitmap']['cluster'] ?? '';
        // //
        // //
        
        // //
        // //
        // //
        // //        
        
        // //
        // //
        // //
        // if (!$this->config['hasDatabase']) {
        //     $this->databasePool = new Pool([]);
        // } else {
        //     $databasePool = $this->storage->getDatabasePool();
        //     $filter = $this->config['database']['poolFilter'] ?? null;
        //     unset($this->config['database']['poolFilter']);
        //     if ($filter !== null) {
        //         $databasePool = $databasePool->filter($filter);
        //     }
        //     $this->databasePool = $databasePool;
        // }
        // //
        // //
        // //
        // if (!$this->config['hasBitmap']) {
        //     $this->bitmapPool = new Pool([]);
        // } else {
        //     $bitmapPool = $this->storage->getBitmapPool();
        //     $filter = $this->config['bitmap']['poolFilter'] ?? null;
        //     unset($this->config['bitmap']['poolFilter']);
        //     if ($filter !== null) {
        //         $bitmapPool = $bitmapPool->filter($filter);
        //     }
        //     $this->bitmapPool = $bitmapPool;
        // }
        // //
        // //
        // //
        // $cache = $this->config['cache'] ?? null;
        // if ($cache !== null) {
        //     $cache += [
        //         'ttl' => 3600,
        //     ];
        //     @ $cache['fieldsToId'] = (array) $cache['fieldsToId'];
        // }
        // $this->config['cache'] = $cache;
        // //
        // //
        // //
        // $this->config['bitmap']['handleValues'] = $this->config['bitmap']['handleValues'] ?? null;
        // //
        // //
        // //
        
        // //
        // //
        // //
        
        // //
        // //
        // //
        
        // //
        // //
        // //
        // /**
    //  * @return Pool
    //  */
    // public function getDatabasePool(): Pool
    // {
    //     $args = func_get_args();
    //     if ($args) {
    //         [$flags, $object] = [$args[0], $args[1] ?? null];
    //         if (!is_object($object)) {
    //             $object = (object) ['id' => $object];
    //         }
    //         $names = $this->databasePool->names();
    //         if ($this->databaseCluster === null) {
    //             $cluster = $this->config['database']['cluster'];
    //             $cluster = Pool::POOL_CLUSTER_DEFAULT_S . $cluster;
    //             $cluster = Pool::POOL_CLUSTER_DEFAULT_P . $cluster;
    //             $cluster = Pool::POOL_CLUSTER_DEFAULT_W . $cluster;
    //             preg_match_all('~(\w):(.*?)(;|$)~', $cluster, $matches, PREG_SET_ORDER);
    //             $collect = [];
    //             foreach ($matches as $match) {
    //                 $collect[$match[1]] = $match[2];
    //             }
    //             $filter = Pool::convertNotationToFilter($collect['w']);
    //             $this->databaseCluster[Pool::POOL_WRITABLE] = $_ = $this->databasePool->filter($filter);
    //             $filter = Pool::convertNotationToFilter($collect['p']);
    //             $this->databaseCluster[Pool::POOL_PRIMARY] = $_->filter($filter);
    //             $filter = Pool::convertNotationToFilter($collect['p'], true);
    //             $this->databaseCluster[Pool::POOL_SECONDARY] = $_->filter($filter);
    //             $this->databaseCluster['s'] = explode(':', $collect['s']);
    //             $this->databaseCluster['s'][0] = (int) $this->databaseCluster['s'][0];
    //             $this->databaseCluster['s'][1] = explode(',', $this->databaseCluster['s'][1]);
    //         }
    //         $pool = $this->databaseCluster[$flags & (~Pool::POOL_RANDOM)];
    //         if (!$pool->names()) {
    //             return $pool;
    //         }
    //         
    //     }
    //     return $this->databasePool;
    // }

    // // /**
    // //  * @param string $name
    // //  * @param array  $arguments
    // //  *
    // //  * @return mixed
    // //  */
    // // public function __call(string $name, array $arguments)
    // // {
    // //     $closure = function ($one, $two) {
    // //         return function ($smth = null) use ($one, $two) {
    // //             $filter = $this->config[$one][$two . 'PoolFilter'] ?? null;
    // //             $pool = $one . 'Pool';
    // //             $pool = $this->$pool;
    // //             if ($filter === null) {
    // //                 return $pool;
    // //             }
    // //             return $pool->filter($filter($smth, $pool->names()));
    // //         };
    // //     };
    // //     $this->_map = $this->_map ?? [
    // //         'hasDatabase' => function () {
    // //             return $this->config['hasDatabase'];
    // //         },
    // //         'hasBitmap' => function () {
    // //             return $this->config['hasBitmap'];
    // //         },
    // //         
    // //         //
    // //         
    // //         'getSortScore' => function ($bean) {
    // //             $getSortScore = $this->config['database']['getSortScore'] ?? null;
    // //             return $getSortScore !== null ? $getSortScore($bean) : 0;
    // //         },
    // //         'getDatabasePk' => function () {
    // //             return $this->config['database']['pk'];
    // //         },
    // //         'getDatabasePkIncrementBy' => function ($name) {
    // //             $get = $this->config['database']['getPkIncrementBy'] ?? null;
    // //             if ($get === null) {
    // //                 return 1;
    // //             }
    // //             return (int) $get($name, $this->databasePool->names());
    // //         },
    // //         'getDatabasePkStartWith' => function ($name) {
    // //             $get = $this->config['database']['getPkStartWith'] ?? null;
    // //             if ($get === null) {
    // //                 return 1;
    // //             }
    // //             return (int) $get($name, $this->databasePool->names());
    // //         },
    
    // //         //
    // //         'getPrimaryDatabasePool' => $closure('database', 'primary'),
    // //         'getWritableDatabasePool' => $closure('database', 'writable'),
    // //         'getReadableDatabasePool' => $closure('database', 'readable'),
    // //         'getPrimaryBitmapPool' => $closure('bitmap', 'primary'),
    // //         'getWritableBitmapPool' => $closure('bitmap', 'writable'),
    // //         'getReadableBitmapPool' => $closure('bitmap', 'readable'),
    // //         //
    
    // //         'getDatabaseBeanWithFields' => function ($id, $fields) {
    // //             $bean = $this->config['database']['bean'] ?? DatabaseBean::class;
    // //             return new $bean($this, $id, $fields);
    // //         },
    // //         'getBitmapBean' => function ($id, $values = []) {
    // //             return $this->getBitmapBeanWithValues($id, $values);
    // //         },
    // //         'getBitmapBeanWithValues' => function ($id, $values) {
    // //             $bean = $this->getBitmapBeanWithFields($id, $this->getBitmapFields());
    // //             $bean->setValues($values);
    // //             return $bean;
    // //         },
    // //         'getBitmapBeanWithFields' => function ($id, $fields) {
    // //             $bean = $this->config['bitmap']['bean'] ?? BitmapBean::class;
    // //             return new $bean($this, $id, $fields);
    // //         },
    // //         'getBitmapBeanFromDatabaseBean' => function ($bean) {
    // //             $id = (int) $bean->id;
    // //             $getValues = $this->config['bitmap']['getValues'] ?? null;
    // //             if ($getValues === null) {
    // //                 $keys = array_keys($this->config['bitmap']['fields']);
    // //                 $values = array_intersect_key($bean->getValues(), array_flip($keys));
    // //             } else {
    // //                 $values = $getValues($bean);
    // //             }
    // //             return $this->getBitmapBeanWithValues($id, $values);
    // //         },
    // //     ];
    // //     $method = $this->_map[$name] ?? null;
    // //     if ($method === null) {
    // //         throw new RuntimeException(sprintf(self::METHOD_NOT_FOUND, $name));
    // //     }
    // //     return $method(...$arguments);
    // // }
    
    // /**
    //  * @return Storage
    //  */
    // public function getStorage(): Storage
    // {
    //     return $this->storage;
    // }
    // $poolFilter = $params['poolFilter'] ?? null;
            // unset($params['poolFilter']);
            // if ($poolFilter !== null) {
            //     $pool = $pool->filter($poolFilter);
            // }
            // foreach ($pool->names() as $name) {
            //     $iterators[] = $pool->instance($name)->iterate($table, $params);
            // }
            // if (count($iterators) > 1) {
            // } else {
            //     [$iterator] = $iterators;
            // }
            // $ids = [];
        // $coroutine->onResolve(function () use ($emitter) {
        //     $emitter->finish();
        // });
        // return $emitter;
        // $emit = function ($emit) use ($params) {
            
        // };
        // return new Producer($emit);
    