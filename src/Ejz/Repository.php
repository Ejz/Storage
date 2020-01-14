<?php

namespace Ejz;

use Amp\Promise;
use Amp\Deferred;

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

    /** @var array */
    protected $foreignKeys;

    /**
     * @param Storage $storage
     * @param string  $name
     * @param array   $config
     */
    public function __construct(Storage $storage, string $name, array $config) {
        $this->storage = $storage;
        $this->name = $name;
        $this->config = $config;
        $this->fields = $this->getNormalizedFields();
        $this->indexes = $this->getNormalizedIndexes();
        $this->foreignKeys = $this->getNormalizedForeignKeys();
        unset(
            $this->config['fields'],
            $this->config['indexes'],
            $this->config['foreignKeys']
        );
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
            $promises = $this->getWritablePool()->insertAsync($this, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
                $ids = $err ? [0] : array_values($res);
                $min = min($ids);
                $max = max($ids);
                $deferred->resolve($min === $max ? $min : 0);
            });
            return $deferred->promise();
        }, $values);
    }

    /**
     * @param int    $id
     * @param ?array $fields (optional)
     *
     * @return Promise
     */
    public function getAsync(int $id, ?array $fields = null): Promise
    {
        return \Amp\call(function ($id, $fields) {
            $table = $this->getTable();
            $db = $this->getReadablePool()->random();
            $params = ['fields' => $this->getFields($fields)];
            $all = iterator_to_array($db->get($table, [$id], $params));
            if (!$all) {
                return null;
            }
            $row = current($all);
            
            return $deferred->promise();
        }, $id, $fields);
    }

    // /**
    //  * @param array ...args
    //  *
    //  * @return Promise
    //  */
    // public function getAsync(...$args): Promise
    // {
    //     return \Amp\call(function ($args) {
    //         $ids = $args;
    //         $fields = null;
    //         $last = array_pop($ids);
    //         if (is_numeric($last)) {
    //             $ids[] = $last;
    //         } else {
    //             $fields = $last;
    //         }
    //         if (!$ids) {
    //             throw new RuntimeException(self::INVALID_ARGUMENT_FORMAT);
    //         }
    //         $definition = $this->getTableDefinition();
    //         $isCacheable = $definition->isCacheable();
    //         $table = $definition->getTable();
    //         $bean = $definition->getBean();
    //         $result = [];
    //         $meta = [];
    //         $dbs = ['ids' => [], 'map' => []];
    //         foreach ($ids as $id) {
    //             $v = null;
    //             if ($isCacheable) {
    //                 $fields_md5 = $fields_md5 ?? md5(serialize($fields));
    //                 $ck = $table . '.' . $id . '.' . $fields_md5;
    //                 $v = $this->cache->get($ck);
    //                 if ($v !== null) {
    //                     $result[$id] = $v;
    //                 } else {
    //                     $ct = [$table, $table . '.' . $id];
    //                     $meta[$id] = [$ck, $ct];
    //                 }
    //             }
    //             if ($v === null) {
    //                 $db = $this->getReadShardsById($id)->filter(function ($name) use ($definition) {
    //                     return !$definition->isForeignKeyTable($name);
    //                 })->random();
    //                 $name = $db->getName();
    //                 $dbs['map'][$name] = $db;
    //                 $dbs['ids'][$name][] = $id;
    //             }
    //         }
    //         if (!$dbs['map']) {
    //             return $bean === null ? $result : $this->toBeans($bean, $result);
    //         }
    //         $fields = $fields ?? array_keys($definition->getFields());
    //         $fields = array_fill_keys((array) $fields, null);
    //         $fields = $definition->normalizeValues($fields);
    //         $promises = [];
    //         foreach ($dbs['map'] as $name => $db) {
    //             $promises[] = $db->getAsync($definition, $dbs['ids'][$name], $fields);
    //         }
    //         $values = yield $promises;
    //         foreach (($isCacheable ? $values : []) as $value) {
    //             foreach ($value as $id => $v) {
    //                 [$ck, $ct] = $meta[$id];
    //                 $this->cache->set($ck, $v, 3600, $ct);
    //             }
    //         }
    //         $result = array_replace($result, ...$values);
    //         return $bean === null ? $result : $this->toBeans($bean, $result);
    //     }, $args);
    // }

    /**
     * @return array
     */
    private function getNormalizedFields(): array
    {
        $fields = $this->config['fields'] ?? [];
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $collect[$name] = new Field($name, $field['type']);
        }
        return $collect;
    }

    /**
     * @return array
     */
    private function getNormalizedIndexes(): array
    {
        $indexes = $this->config['indexes'] ?? [];
        $collect = [];
        foreach ($indexes as $index) {
        }
        return $collect;
    }

    /**
     * @return array
     */
    private function getNormalizedForeignKeys(): array
    {
        $foreignKeys = $this->config['foreignKeys'] ?? [];
        $collect = [];
        foreach ($foreignKeys as $foreignKey) {
        }
        return $collect;
    }

    /**
     * @return DatabasePool
     */
    private function getWritablePool(): DatabasePool
    {
        $filter = $this->config['writablePoolFilter'] ?? null;
        $pool = $this->getPool();
        return $filter === null ? $pool : $pool->filter($filter);
    }

    /**
     * @return DatabasePool
     */
    private function getReadablePool(): DatabasePool
    {
        $filter = $this->config['readablePoolFilter'] ?? null;
        $pool = $this->getPool();
        return $filter === null ? $pool : $pool->filter($filter);
    }

    /**
     * @return DatabasePool
     */
    public function getPool(): DatabasePool
    {
        $filter = $this->config['poolFilter'] ?? null;
        $pool = $this->storage->getPool();
        return $filter === null ? $pool : $pool->filter($filter);
    }

    /**
     * @return int
     */
    public function getPkIncrementBy(): int
    {
        $pkIncrementBy = $this->config['pkIncrementBy'] ?? null;
        return $pkIncrementBy ?? 1;
    }

    /**
     * @return int
     */
    public function getPkStartWith(): int
    {
        $pkStartWith = $this->config['pkStartWith'] ?? null;
        return $pkStartWith ?? 1;
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
     * @param ?array $fields (optional)
     *
     * @return array
     */
    public function getFields(?array $fields = null): array
    {
        return $fields === null ? $this->fields : [];
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
     * @param string $name
     *
     * @return bool
     */
    public function isForeignKeyTable(string $name): bool
    {
        $_ = $this->config['isForeignKeyTable'] ?? null;
        return $_ === null ? false : $_($name, $this->getPool()->names());
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
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
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
    public function createAsync(): Promise
    {
        return \Amp\call(function () {
            yield $this->getPool()->createAsync($this);
        });
    }

    /**
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
            yield $this->getPool()->dropAsync($this->getTable());
        });
    }

    /**
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
            yield $this->getPool()->truncateAsync($this->getTable());
        });
    }

    /**
     */
    public function truncate()
    {
        \Amp\Promise\wait($this->truncateAsync());
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
}
