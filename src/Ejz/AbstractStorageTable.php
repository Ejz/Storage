<?php

namespace Ejz;

use Error;
use Amp\Loop;
use Amp\Promise;
use Amp\Deferred;
use RuntimeException;

abstract class AbstractStorageTable
{
    /** @var DatabasePool */
    private $pool;

    /** @var TableDefinition */
    private $definition;

    /**
     * @param DatabasePool    $pool
     * @param TableDefinition $definition
     */
    public function __construct(
        DatabasePool $pool,
        TableDefinition $definition
    ) {
        $this->pool = $pool;
        $this->definition = $definition;
    }

    /**
     * @return Promise
     */
    public function truncateAsync(): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred) {
            $table = $this->definition->getTable();
            $ret = yield $this->pool->truncateAsync($table);
            $deferred->resolve($ret);
        });
        return $promise;
    }

    /**
     * @return array
     */
    public function truncate(): array
    {
        Loop::run(function () use (&$ret) {
            $ret = yield $this->truncateAsync();
        });
        return $ret;
    }

    /**
     * @return Promise
     */
    public function createAsync(): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred) {
            $ret = yield $this->pool->createAsync($this->definition);
            $deferred->resolve($ret);
        });
        return $promise;
    }

    /**
     *
     */
    public function create()
    {
        Loop::run(function () use (&$ret) {
            $ret = yield $this->createAsync();
        });
        return $ret;
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insertAsync(array $values = []): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $values) {
            $values = $this->definition->setDefaultValues($values);
            $pool = $this->getShards(null, $values);
            $promise = $pool->insertAsync($this->definition, $values);
            $promise->onResolve(function ($err, $res) use ($deferred) {
                // var_dump($err, $res);
                if ($err) {
                    $deferred->resolve(0);
                    return;
                }
                $ids = array_values($res);
                $id = array_pop($ids);
                foreach ($ids as $_) {
                    if ($_ !== $id) {
                        throw new RuntimeException();
                    }
                }
                $deferred->resolve($id);
            });
        });
        return $promise;
    }

    /**
     * @param array $values (optional)
     *
     * @return int
     */
    public function insert(array $values = []): int
    {
        Loop::run(function () use (&$ret, $values) {
            $ret = yield $this->insertAsync($values);
        });
        return $ret;
        try {
        } catch (Error $e) {
            return 0;
        }
    }

    /**
     * @param int $id
     *
     * @return Promise
     */
    public function getAsync(int $id): Promise
    {
        $deferred = new Deferred();
        $promise = $deferred->promise();
        Loop::defer(function () use ($deferred, $id) {
            $pool = $this->getShard($id, []);
            $ret = yield $pool->getAsync($this->definition, $id);
            $one = array_values($ret)[0];
            $deferred->resolve($one);
        });
        return $promise;
    }

    /**
     * @param int $id
     *
     * @return array
     */
    public function get(int $id): array
    {
        Loop::run(function () use (&$ret, $id) {
            $ret = yield $this->getAsync($id);
        });
        return $ret;
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return DatabasePool
     */
    public function getShards(?int $id, array $values = []): DatabasePool
    {
        $shards = $this->definition->getShards($id, $values);
        return $this->pool->dbs($shards);
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return DatabasePool
     */
    public function getShard(?int $id, array $values = []): DatabasePool
    {
        $shard = $this->definition->getShard($id, $values);
        return $this->pool->dbs([$shard]);
    }

    /**
     * @param int $id
     *
     * @return DatabaseInterface
     */
    // public function getDbFromId(int $id): DatabaseInterface
    // {
    //     $shards = $this->definition['shards'];
    //     $n = $id % $shards;
    //     return $this->pool[$n];
    // }

    /**
     * @param array $values
     *
     * @return DatabaseInterface
     */
    // public function getDbFromValues(array $values): DatabaseInterface
    // {
    //     $shards = $this->definition['shards'];
    //     $value = $values[$this->definition['shard_value']];
    //     $n = crc32($value) % $shards;
    //     return $this->pool[$n];
    // }

    // $storage->user()->get(1)
    // $storage->user()->iterator()
    // $storage->user()->all(1)
    // $storage->user()->one(1)
    // $storage->user()->val(1)
    // $storage->user()->col(1)
    // $storage->user()->col(1)
    // $storage->user([1])
    // $storage->user([1, 2])
}