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

    /** @var array */
    protected $fields;

    /** @var array */
    protected $indexes;

    /** @var DatabasePool */
    protected $pool;

    /** @var ?array */
    protected $cache;

    /** @var ?array */
    protected $bitmap;

    /**
     * @param Storage $storage
     * @param string  $name
     * @param array   $config
     */
    public function __construct(Storage $storage, string $name, array $config) {
        $this->storage = $storage;
        $this->name = $name;
        $this->config = $config + [
            'iterator_chunk_size' => 100,
        ];
        $this->normalize();
        $this->normalizeCache();
        $this->normalizeBitmap();
        $this->pool = $this->getPool();
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function isPrimaryTable(string $name): bool
    {
        $callable = $this->config['isPrimaryTable'] ?? null;
        if ($callable === null) {
            return true;
        }
        $names = $this->pool->names();
        return (bool) $callable($name, $names);
    }

    /**
     * @param string $name
     *
     * @return int
     */
    public function getPkIncrementBy(string $name): int
    {
        $callable = $this->config['getPkIncrementBy'] ?? null;
        if ($callable === null) {
            return 1;
        }
        $names = $this->pool->names();
        return (int) $callable($name, $names);
    }

    /**
     * @param string $name
     *
     * @return int
     */
    public function getPkStartWith(string $name): int
    {
        $callable = $this->config['getPkStartWith'] ?? null;
        if ($callable === null) {
            return 1;
        }
        $names = $this->pool->names();
        return (int) $callable($name, $names);
    }

    /**
     * @param ?int   $id     (optional)
     * @param ?array $values (optional)
     *
     * @return DatabasePool
     */
    private function getWritablePool(?int $id = null, ?array $values = null): DatabasePool
    {
        $pool = $this->pool;
        $callable = $this->config['getWritablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        $nargs = func_num_args();
        return $pool->filter($callable($id, $values, $pool->names(), $nargs));
    }

    /**
     * @param ?int   $id     (optional)
     * @param ?array $values (optional)
     *
     * @return DatabasePool
     */
    private function getReadablePool(?int $id = null, ?array $values = null): DatabasePool
    {
        $pool = $this->pool;
        $callable = $this->config['getReadablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        $nargs = func_num_args();
        return $pool->filter($callable($id, $values, $pool->names(), $nargs));
    }

    /**
     * @return DatabasePool
     */
    public function getPool(): DatabasePool
    {
        $pool = $this->storage->getPool();
        $callable = $this->config['getPool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        return $pool->filter($callable);
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
     * @return bool
     */
    public function hasBitmap(): bool
    {
        return $this->bitmap !== null;
    }

    /**
     * @return bool
     */
    public function hasCache(): bool
    {
        return $this->cache !== null;
    }

    /**
     * @return string
     */
    public function getTable(): string
    {
        $table = $this->config['table'] ?? null;
        return $table ?? strtolower($this->name);
    }

    /**
     * @return Promise
     */
    public function create(): Promise
    {
        return Promise\all($this->pool->create($this));
    }

    /**
     * @return Promise
     */
    public function drop(): Promise
    {
        return Promise\all($this->pool->drop($this->getTable()));
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
        $fields = $this->config['fields'] ?? [];
        $indexes = $this->config['indexes'] ?? [];
        $foreignKeys = $this->config['foreignKeys'] ?? [];
        unset($this->config['fields']);
        unset($this->config['indexes']);
        unset($this->config['foreignKeys']);
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $collect[$name] = $field;
        }
        $this->fields = $collect;
        $collect = [];
        foreach ($indexes as $name => $index) {
            if (!$is_assoc($index)) {
                $index = ['fields' => $index];
            }
            $type = $index['type'] ?? null;
            $collect[] = new Index($name, $index['fields'], $type);
        }
        $this->indexes = $collect;
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
        $this->foreignKeys = $collect;
    }

    /**
     */
    private function normalizeCache()
    {
        $cache = $this->config['cache'] ?? null;
        unset($this->config['cache']);
        if ($cache !== null) {
            $cache += [
                'ttl' => 3600,
            ];
            @ $cache['fieldsToId'] = (array) $cache['fieldsToId'];
        }
        $this->cache = $cache;
    }

    /**
     */
    private function normalizeBitmap()
    {
        $bitmap = $this->config['bitmap'] ?? null;
        unset($this->config['bitmap']);
        if ($bitmap !== null) {
            $fields = $bitmap['fields'] ?? [];
            $collect = [];
            foreach ($fields as $name => $field) {
                if ($field instanceof AbstractType) {
                    $field = ['type' => $field];
                }
                $collect[$name] = $field;
            }
            $fields = $collect;
            $handleValues = $bitmap['handleValues'] ?? null;
            $bitmap = compact('fields', 'handleValues');
        }
        $this->bitmap = $bitmap;
    }

    /**
     * @return string
     */
    public function getPk(): string
    {
        $pk = $this->config['pk'] ?? null;
        return $pk ?? $this->getTable() . '_id';
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        $fields = [];
        foreach ($this->fields as $name => $field) {
            $fields[$name] = new Field($name, $field['type']);
        }
        return $fields;
    }

    /**
     * @return array
     */
    public function getBitmapFields(): array
    {
        $fields = [];
        foreach (($this->bitmap['fields'] ?? []) as $name => $field) {
            $fields[$name] = new Field($name, $field['type']);
        }
        return $fields;
    }

    /**
     * @return array
     */
    public function getIndexes(): array
    {
        return $this->indexes;
    }

    /**
     * @return array
     */
    public function getForeignKeys(): array
    {
        return $this->foreignKeys;
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insert(array $values = []): Promise
    {
        $bean = $this->getBean(null, $values);
        return $this->insertBean($bean);
    }

    /**
     * @param Bean $bean
     *
     * @return Promise
     */
    public function insertBean(Bean $bean): Promise
    {
        $deferred = new Deferred();
        $fields = $bean->getFields();
        $values = $bean->getValues();
        $promises = $this->getWritablePool(null, $values)->insert($this, $fields);
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $ids = $err ? [0] : array_values($res);
            $min = min($ids);
            $max = max($ids);
            $deferred->resolve($min === $max ? $min : null);
        });
        return $deferred->promise();
    }

    /**
     * @param array $values (optional)
     *
     * @return bool
     */
    public function bitmapAdd(int $id, array $values = []): bool
    {
        $bean = $this->getBitmapBean($id, $values);
        return $this->bitmapAddBean($bean);
    }

    /**
     * @param BitmapBean $bean
     *
     * @return bool
     */
    public function bitmapAddBean(BitmapBean $bean): bool
    {
        $table = $this->getTable();
        $bitmap = $this->storage->getBitmap();
        return $bitmap->ADD($table, $bean->getId(), $bean->getFields());
    }

    /**
     * @param array  $ids
     *
     * @return Iterator
     */
    public function get(array $ids): Iterator
    {
        $emit = function ($emit) use ($ids) {
            if (!$ids) {
                return;
            }
            $table = $this->getTable();
            $dbs = [];
            $meta = [];
            foreach ($ids as $id) {
                if ($this->cache !== null) {
                    $ck = $table . '.' . $id;
                    $cache = $cache ?? $this->storage->getCache();
                    $v = $cache->get($ck);
                    if ($v !== null) {
                        [$id, $fields] = $v;
                        $bean = $this->getBeanWithFields($id, $fields);
                        yield $emit([$id, $bean]);
                        continue;
                    }
                    $ct = [$table, $table . '.' . $id];
                    $meta[$id] = [$ck, $ct];
                }
                $db = $this->getReadablePool($id)->random();
                $name = $db->getName();
                $dbs[$name] = $dbs[$name] ?? ['db' => $db, 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
            if (!$dbs) {
                return;
            }
            $fields = array_values($this->getFields());
            [$returnFields, $pk] = [true, [$this->getPk()]];
            $params = compact('fields', 'pk', 'returnFields');
            $iterators = [];
            foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
                $iterators[] = $db->get($table, $ids, $params);
            }
            $iterator = count($iterators) === 1 ? $iterators[0] : Producer::merge($iterators, []);
            while (yield $iterator->advance()) {
                [$id, $fields] = $v = $iterator->getCurrent();
                $bean = $this->getBeanWithFields($id, $fields);
                if ($this->cache !== null) {
                    [$ck, $ct] = $meta[$id];
                    $cache = $cache ?? $this->storage->getCache();
                    $cache->set($ck, $v, $this->cache['ttl'], $ct);
                    foreach ($this->cache['fieldsToId'] as $f2id) {
                        $v = md5(serialize($bean->$f2id));
                        $ck = $table . '.' . $f2id . '.' . $v;
                        $cache->set($ck, $id, $this->cache['ttl'], $ct);
                    }
                }
                yield $emit([$id, $bean]);
            }
        };
        return new Producer($emit);
    }

    /**
     * @param array $params (optional)
     *
     * @return Iterator
     */
    public function iterate(array $params = []): Iterator
    {
        $emit = function ($emit) use ($params) {
            $params += [
                'fields' => null,
                'returnFields' => true,
                'poolFilter' => null,
            ];
            [
                'fields' => $fields,
                'returnFields' => $returnFields,
                'poolFilter' => $poolFilter,
            ] = $params;
            $fields = $fields ?? array_values($this->getFields());
            $pk = [$this->getPk()];
            $params = compact('fields', 'pk') + $params;
            $table = $this->getTable();
            $pool = $this->getReadablePool();
            if ($poolFilter !== null) {
                $pool = $pool->filter($poolFilter);
            }
            $iterators = [];
            foreach ($pool->names() as $name) {
                $iterators[] = $pool->db($name)->iterate($table, $params);
            }
            $iterator = count($iterators) === 1 ? $iterators[0] : Producer::merge($iterators, $params);
            while (yield $iterator->advance()) {
                [$id, $fields] = $iterator->getCurrent();
                $ret = $returnFields ? $this->getBeanWithFields($id, $fields) : $fields;
                yield $emit([$id, $ret]);
            }
        };
        return new Producer($emit);
    }

    /**
     * @param array $conditions
     *
     * @return Iterator
     */
    public function filter(array $conditions): Iterator
    {
        if (
            $this->cache !== null &&
            count($conditions) === 1 &&
            in_array($f = key($conditions), $this->cache['fieldsToId'])
        ) {
            $value = current($conditions);
            if (!is_array($value)) {
                $table = $this->getTable();
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
     * @param string $query
     *
     * @return Iterator
     */
    public function search(string $query): Iterator
    {
        [$size, $cursor] = $this->bitmapSearch($query);
        $iterator_chunk_size = $this->config['iterator_chunk_size'];
        $emit = function ($emit) use ($size, $cursor, $iterator_chunk_size) {
            while ($size > 0) {
                $bitmap = $bitmap ?? $this->storage->getBitmap();
                $ids = $bitmap->CURSOR($cursor, 'LIMIT', $iterator_chunk_size);
                $size -= $iterator_chunk_size;
                $iterator = $this->get($ids);
                while (yield $iterator->advance()) {
                    yield $emit($iterator->getCurrent());
                }
            }
        };
        $iterator = new Producer($emit);
        $iterator->setSize($size);
        return $iterator;
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
     * @param array $ids
     * @param array $fields
     *
     * @return Promise
     */
    public function update(array $ids, array $fields): Promise
    {
        if (!$ids || !$fields) {
            return new Success(0);
        }
        $deferred = new Deferred();
        $dbs = [];
        foreach ($ids as $id) {
            if ($this->cache !== null) {
                $table = $table ?? $this->getTable();
                $cache = $cache ?? $this->storage->getCache();
                $cache->drop($table . '.' . $id);
            }
            $pool = $this->getWritablePool($id, null);
            foreach ($pool->names() as $name) {
                $dbs[$name] = $dbs[$name] ?? ['db' => $pool->db($name), 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
        }
        $promises = [];
        foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
            $promises[] = $db->update($this, $ids, $fields);
        }
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $deferred->resolve($err ? 0 : array_sum($res));
        });
        return $deferred->promise();
    }

    /**
     * @param array $ids
     *
     * @return Promise
     */
    public function delete(array $ids): Promise
    {
        if (!$ids) {
            return new Success(0);
        }
        $deferred = new Deferred();
        $dbs = [];
        foreach ($ids as $id) {
            if ($this->cache !== null) {
                $table = $table ?? $this->getTable();
                $cache = $cache ?? $this->storage->getCache();
                $cache->drop($table . '.' . $id);
            }
            $pool = $this->getWritablePool($id, null);
            foreach ($pool->names() as $name) {
                $dbs[$name] = $dbs[$name] ?? ['db' => $pool->db($name), 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
        }
        $promises = [];
        foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
            $promises[] = $db->delete($this, $ids);
        }
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $deferred->resolve($err ? 0 : array_sum($res));
        });
        return $deferred->promise();
    }

    /**
     * @param int $id1
     * @param int $id2
     *
     * @return Promise
     */
    public function reid(int $id1, int $id2): Promise
    {
        $pool1 = $this->getWritablePool($id1);
        $pool2 = $this->getWritablePool($id2);
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
        $getSortScore = $this->config['getSortScore'] ?? null;
        if ($getSortScore === null) {
            return false;
        }
        $names = $this->pool->names();
        $ok = 0;
        foreach ($names as $name) {
            $scores = [];
            $generator = $this->iterate(['poolFilter' => $name])->generator();
            foreach ($generator as $id => $bean) {
                $scores[$id] = $getSortScore($bean->getValues());
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
    public function bitmapCreate()
    {
        $this->storage->getBitmap()->CREATE($this);
    }

    /**
     */
    public function bitmapDrop()
    {
        $table = $this->getTable();
        $this->storage->getBitmap()->DROP($table);
    }

    /**
     * @param string $cursor
     * @param int    $limit
     *
     * @return array
     */
    public function bitmapCursor(string $cursor, int $limit): array
    {
        return $this->storage->getBitmap()->CURSOR($cursor, 'LIMIT', $limit);
    }

    /**
     * @param string $query
     *
     * @return array
     */
    public function bitmapSearch(string $query): array
    {
        $table = $this->getTable();
        $result = $this->storage->getBitmap()->SEARCH($table, $query, 'WITHCURSOR');
        return [$result[0] ?? 0, $result[1] ?? null];
    }

    /**
     */
    public function bitmapPopulate()
    {
        $handleValues = $this->bitmap['handleValues'] ?? null;
        $generator = $this->bitmap === null ? [] : $this->iterate()->generator();
        $table = $this->getTable();
        $keys = array_keys($this->getBitmapFields());
        foreach ($generator as $id => $bean) {
            $values = $bean->getValues();
            if ($handleValues !== null) {
                $values = $handleValues($values);
            } else {
                $values = array_intersect_key($values, array_flip($keys));
            }
            $this->getBitmapBean($id, $values)->add();
        }
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return Bean
     */
    private function getBitmapBean(int $id, array $values = []): BitmapBean
    {
        $bean = $this->getBitmapBeanWithFields($id, $this->getBitmapFields());
        $bean->setValues($values);
        return $bean;
    }

    /**
     * @param ?int  $id     (optional)
     * @param array $values (optional)
     *
     * @return Bean
     */
    public function getBean(?int $id = null, array $values = []): Bean
    {
        $bean = $this->getBeanWithFields($id, $this->getFields());
        $bean->setValues($values);
        return $bean;
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
     * @param ?int  $id
     * @param array $fields
     *
     * @return Bean
     */
    private function getBeanWithFields(?int $id, array $fields): Bean
    {
        $bean = $this->config['bean'] ?? Bean::class;
        return new $bean($this, $id, $fields);
    }

    /**
     * @param int   $id
     * @param array $fields
     *
     * @return BitmapBean
     */
    private function getBitmapBeanWithFields(int $id, array $fields): BitmapBean
    {
        $bean = $this->bitmap['bean'] ?? BitmapBean::class;
        return new $bean($this, $id, $fields);
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
     * @param array ...$args
     *
     * @return mixed
     */
    public function dropSync(...$args)
    {
        return Promise\wait($this->drop(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function insertSync(...$args)
    {
        return Promise\wait($this->insert(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function updateSync(...$args)
    {
        return Promise\wait($this->update(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function deleteSync(...$args)
    {
        return Promise\wait($this->delete(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function existsSync(...$args)
    {
        return Promise\wait($this->exists(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function reidSync(...$args)
    {
        return Promise\wait($this->reid(...$args));
    }
}
