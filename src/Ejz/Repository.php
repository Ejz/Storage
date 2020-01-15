<?php

namespace Ejz;

use Amp\Promise;
use Amp\Deferred;
use Amp\Iterator;

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
            $collect[$name] = new Field($name, $field['type']);
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
     * @return string
     */
    public function getPk(): string
    {
        $pk = $this->config['pk'] ?? null;
        return $pk ?? $this->getTable() . '_id';
    }

    /**
     * @param ?array $filter (optional)
     *
     * @return array
     */
    public function getFields(?array $filter = null): array
    {
        if ($filter === null) {
            return $this->fields;
        }
        return array_intersect_key($this->fields, array_flip((array) $filter));
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
     * @param Bean|array $values (optional)
     *
     * @return Promise
     */
    public function insert($values = []): Promise
    {
        return \Amp\call(function ($values) {
            $deferred = new Deferred();
            $bean = is_array($values) ? $this->convertValuesToBean($values) : $values;
            $promises = $this->getWritablePool()->insert($this, $bean->getFields());
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
     * @param array  $ids
     * @param ?array $fields (optional)
     *
     * @return Iterator
     */
    public function get(array $ids, ?array $fields = null): Iterator
    {
        $emit = function ($emit) use ($ids, $fields) {
            $table = $this->getTable();
            $db = $this->getReadablePool()->random();
            $fields = array_values($this->getFields($fields));
            $params = ['fields' => $fields, 'returnFields' => true];
            $iterator = $db->get($table, $ids, $params);
            while (yield $iterator->advance()) {
                [$id, $fields] = $iterator->getCurrent();
                $bean = $this->getBean($id, $fields);
                yield $emit($bean);
            }
        };
        return new class($emit) extends Producer {
            use SimpleGeneratorTrait;
        };
    }

    /**
     * @param ?int  $id
     * @param array $fields
     *
     * @return Bean
     */
    public function getBean(?int $id, array $fields): Bean
    {
        $bean = $this->config['bean'] ?? Bean::class;
        return new $bean($this, $id, $fields);
    }

    /**
     * @param array $values
     *
     * @return Bean
     */
    private function convertValuesToBean(array $values): Bean
    {
        $fields = $this->getFields();
        foreach ($values as $key => $value) {
            $fields[$key]->setValue($value);
        }
        return $this->getBean(null, $fields);
    }
}
