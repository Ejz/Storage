<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;
use Amp\Iterator;
use RuntimeException;

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
    public function isPrimaryTable(string $name): bool
    {
        $callable = $this->config['isPrimaryTable'] ?? null;
        if ($callable === null) {
            return true;
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
     * @param ?int   $id
     * @param ?array $values
     *
     * @return DatabasePool
     */
    private function getWritablePool(?int $id, ?array $values): DatabasePool
    {
        $pool = $this->getPool();
        $callable = $this->config['getWritablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        return $pool->filter($callable($id, $values, $pool->names()));
    }

    /**
     * @param ?int   $id
     * @param ?array $values
     *
     * @return DatabasePool
     */
    private function getReadablePool(?int $id, ?array $values): DatabasePool
    {
        $pool = $this->getPool();
        $callable = $this->config['getReadablePool'] ?? null;
        if ($callable === null) {
            return $pool;
        }
        return $pool->filter($callable($id, $values, $pool->names()));
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
        $deferred = new Deferred();
        $fields = $bean->getFields();
        $values = $bean->getValues();
        $promises = $this->getWritablePool(null, $values)->insert($this, $fields);
        Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
            $ids = $err ? [0] : array_values($res);
            $min = min($ids);
            $max = max($ids);
            $deferred->resolve($min === $max ? $min : 0);
        });
        return $deferred->promise();
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
            foreach ($ids as $id) {
                $db = $this->getReadablePool($id, null)->random();
                $name = $db->getName();
                $dbs[$name] = $dbs[$name] ?? ['db' => $db, 'ids' => []];
                $dbs[$name]['ids'][] = $id;
            }
            $fields = array_values($this->getFields());
            [$returnFields, $pk] = [true, [$this->getPk()]];
            $params = compact('fields', 'pk', 'returnFields');
            $iterators = [];
            foreach ($dbs as ['db' => $db, 'ids' => $ids]) {
                $iterators[] = $db->get($table, $ids, $params);
            }
            $iterator = count($iterators) === 1 ? $iterators[0] : Iterator\merge($iterators);
            while (yield $iterator->advance()) {
                [$id, $fields] = $iterator->getCurrent();
                $bean = $this->getBeanWithFields($id, $fields);
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
        if (!$ids) {
            return Success(0);
        }
        $deferred = new Deferred();
        $dbs = [];
        foreach ($ids as $id) {
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
            return Success(0);
        }
        $deferred = new Deferred();
        $dbs = [];
        foreach ($ids as $id) {
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
     * @param ?int  $id     (optional)
     * @param array $values (optional)
     *
     * @return Bean
     */
    public function getBean(?int $id = null, array $values = []): Bean
    {
        $bean = $this->getBeanWithFields($id, $this->getFields());
        foreach ($values as $key => $value) {
            $bean->$key = $value;
        }
        return $bean;
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
    public function truncateSync(...$args)
    {
        return Promise\wait($this->truncate(...$args));
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
}
