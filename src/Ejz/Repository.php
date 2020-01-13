<?php

namespace Ejz;

use Amp\Promise;

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
     * @param mixed $values (optional)
     *
     * @return Promise
     */
    public function insertAsync($values = []): Promise
    {
        return \Amp\call(function ($values) {
            $deferred = new Deferred();
            // $shards = $this->getWriteShardsByValues($values);
            $fields = $definition->normalizeValues($values);
            $promises = $shards->insertAsync($definition, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred, $definition, $values) {
                $ids = $err ? [0] : array_values($res);
                $min = min($ids);
                $max = max($ids);
                if ($min !== $max) {
                    throw new RuntimeException(sprintf(self::INVALID_INSERTED_IDS, $min, $max));
                }
                if ($definition->hasBitmap() && $min > 0) {
                    $this->bitmap->upsert($definition, $min, $values);
                }
                $deferred->resolve($min);
            });
            return $deferred->promise();
        }, $values);
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
    private function getPool(): DatabasePool
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
     * @return array
     */
    public function getFields(): array
    {
        return $this->fields;
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
        return false;
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
}
