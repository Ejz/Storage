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

    /**
     * @param Storage $storage
     * @param string  $name
     * @param array   $config
     */
    public function __construct(Storage $storage, string $name, array $config) {
        $this->storage = $storage;
        $this->name = $name;
        $this->config = $config;
        $this->normalize();
        // $this->fields = $this->getNormalizedFields();
        // $this->indexes = $this->getNormalizedIndexes();
        // unset($this->config['fields'], $this->config['indexes']);
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function isForeignKeyTable(string $name): bool
    {
        $callable = $this->config['isForeignKeyTable'] ?? null;
        if ($callable === null) {
            return false;
        }
        $names = $this->getPool()->names();
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
        $names = $this->getPool()->names();
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
        $names = $this->getPool()->names();
        return (int) $callable($name, $names);
    }

    /**
     * @return DatabasePool
     */
    private function getWritablePool(): DatabasePool
    {
        $pool = $this->getPool();
        $callable = $this->config['getWritablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        return $pool->filter($callable);
    }

    /**
     * @return DatabasePool
     */
    private function getReadablePool(): DatabasePool
    {
        $pool = $this->getPool();
        $callable = $this->config['getReadablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        return $pool->filter($callable);
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
        return Promise\all($this->getPool()->create($this));
    }

    /**
     * @return Promise
     */
    public function drop(): Promise
    {
        return Promise\all($this->getPool()->drop($this->getTable()));
    }

    /**
     * @return Promise
     */
    public function truncate(): Promise
    {
        return Promise\all($this->getPool()->truncate($this->getTable()));
    }

    /**
     */
    private function normalize()
    {
        $fields = $this->config['fields'] ?? [];
        $indexes = $this->config['indexes'] ?? [];
        unset($this->config['fields']);
        unset($this->config['indexes']);
        $collect = [];
        foreach ($fields as $name => $field) {
            if ($field instanceof AbstractType) {
                $field = ['type' => $field];
            }
            $collect[$name] = new Field($name, $field['type']);
        }
        $this->fields = $collect;
        $collect = [];
        foreach ($indexes as $name => $index) {
            if ($this->isAssocArray($index)) {
                $index = ['fields' => $index];
            }
            $type = $index['type'] ?? null;
            $collect[$name] = new Index($name, $index['fields'], $type);
        }
        $this->indexes = $collect;
    }

    // /**
    //  * @param array $values (optional)
    //  *
    //  * @return Promise
    //  */
    // public function insertAsync(array $values = []): Promise
    // {
    //     return \Amp\call(function ($values) {
    //         $deferred = new Deferred();
    //         $promises = $this->getWritablePool()->insertAsync($this, $values);
    //         \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
    //             $ids = $err ? [0] : array_values($res);
    //             $min = min($ids);
    //             $max = max($ids);
    //             $deferred->resolve($min === $max ? $min : 0);
    //         });
    //         return $deferred->promise();
    //     }, $values);
    // }

    // /**
    //  * @param int    $id
    //  * @param ?array $fields (optional)
    //  *
    //  * @return Promise
    //  */
    // public function getAsync(int $id, ?array $fields = null): Promise
    // {
    //     return \Amp\call(function ($id, $fields) {
    //         $table = $this->getTable();
    //         $db = $this->getReadablePool()->random();
    //         $params = ['fields' => $this->getFields($fields)];
    //         $all = iterator_to_array($db->get($table, [$id], $params));
    //         if (!$all) {
    //             return null;
    //         }
    //         $row = current($all);
            
    //         return $deferred->promise();
    //     }, $id, $fields);
    // }

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
        return [];
    }

    /**
     * @param mixed $array
     *
     * @return bool
     */
    private function isAssocArray($array): bool
    {
        if (!is_array($array)) {
            return false;
        }
        $count0 = count($array);
        $count1 = count(array_filter(array_keys($array), 'is_string'));
        return $count0 === $count1;
    }
}
