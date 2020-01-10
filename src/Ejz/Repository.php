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

    /**
     * @param Storage $storage
     * @param string  $name
     * @param array   $config
     */
    public function __construct(Storage $storage, string $name, array $config) {
        $this->storage = $storage;
        $this->name = $name;
        $this->config = $config;
    }

    public function getStorage(){}
    public function getName(){}
    public function getConfig(){}

    /**
     * @return Promise
     */
    public function createAsync(): Promise
    {
        return \Amp\call(function () {
            yield $this->dropAsync();
            if ($this->hasBitmap()) {
                $this->storage->getBitmap()->CREATE($this);
            }
            yield $this->getPool()->createAsync($this);
        });
    }

    /**
     *
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
            if ($this->isCacheable()) {
                $this->storage->getCache()->drop($this->name);
            }
            yield $this->getPool()->dropAsync($this->name);
        });
    }

    /**
     *
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
            if ($this->isCacheable()) {
                $this->storage->getCache()->drop($this->name);
            }
            yield $this->getPool()->truncateAsync($this->name);
        });
    }

    /**
     *
     */
    public function truncate()
    {
        \Amp\Promise\wait($this->truncateAsync());
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
            $definition = $this->getTableDefinition();
            $shards = $this->getWriteShardsByValues($values);
            $values = $definition->normalizeValues($values);
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
}
