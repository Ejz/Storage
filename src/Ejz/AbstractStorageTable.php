<?php

namespace Ejz;

use Generator;
use Error;
use Amp\Loop;
use Amp\Promise;
use Amp\Deferred;
use Amp\Producer;
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
        return \Amp\call(function () {
            $table = $this->definition->getTable();
            return yield $this->pool->truncateAsync($table);
        });
    }

    /**
     * @return array
     */
    public function truncate(): array
    {
        return \Amp\Promise\wait($this->truncateAsync());
    }

    /**
     * @return Promise
     */
    public function createAsync(): Promise
    {
        return \Amp\call(function () {
            return yield $this->pool->createAsync($this->definition);
        });
    }

    /**
     *
     */
    public function create()
    {
        return \Amp\Promise\wait($this->createAsync());
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insertAsync(array $values = []): Promise
    {
        $values = $this->definition->setDefaultValues($values);
        return \Amp\call(function ($values) {
            $deferred = new Deferred();
            $pool = $this->getShards(null, $values);
            $promises = $pool->insertAsync($this->definition, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
                if ($err) {
                    return $deferred->resolve(0);
                }
                $ids = array_values($res);
                $id = array_pop($ids);
                foreach ($ids as $_) {
                    if ($_ !== $id) {
                        return $deferred->resolve(0);
                    }
                }
                $deferred->resolve($id);
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
     * @param int $id
     *
     * @return Promise
     */
    public function getAsync(int $id): Promise
    {
        $values = $this->definition->setDefaultValues([]);
        return \Amp\call(function ($id, $values) {
            $db = $this->getShard($id, $values);
            return yield $db->getAsync($this->definition, $id);
        }, $id, $values);
    }

    /**
     * @param int $id
     *
     * @return array
     */
    public function get(int $id): array
    {
        return \Amp\Promise\wait($this->getAsync($id));
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterate(array $params = []): Producer
    {
        $table = $this->definition->getTable();
        $iterators = $this->pool->iterate($table, $params);
        return $this->joinIterators($iterators);
    }

    /**
     * @param array $where (optional)
     *
     * @return Promise
     */
    public function selectAsync(array $where = []): Promise
    {
        return \Amp\call(function ($where) {
            $iterator = $this->iterate(compact('where'));
            return yield \Amp\Iterator\toArray($iterator);
        }, $where);
    }

    /**
     * @param array $where (optional)
     *
     * @return array
     */
    public function select(array $where = []): array
    {
        return \Amp\Promise\wait($this->selectAsync($where));
    }

    // /**
    //  * @param int $id
    //  *
    //  * @return Promise
    //  */
    // public function getEverywhereAsync(int $id): Promise
    // {
    //     $values = $this->definition->setDefaultValues([]);
    //     return \Amp\call(function ($id, $values) {
    //         $deferred = new Deferred();
    //         $promises = $this->pool->getAsync($this->definition, $id);
    //         \Amp\Promise\any($promises)->onResolve(function ($err, $res) use ($deferred) {
    //             [, $vals] = $res;
    //             var_dump($vals);
    //             foreach ($vals as $val) {
    //                 if ($val) {
    //                     return $deferred->resolve($val);
    //                 }
    //             }
    //             $deferred->resolve([]);
    //             // if ($err) {
    //             //     return $deferred->resolve(0);
    //             // }
    //             // $ids = array_values($res);
    //             // $id = array_pop($ids);
    //             // foreach ($ids as $_) {
    //             //     if ($_ !== $id) {
    //             //         return $deferred->resolve(0);
    //             //     }
    //             // }
    //             // $deferred->resolve($id);
    //         });
    //         return $deferred->promise();
    //     }, $id, $values);
    // }

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
     * @return DatabaseInterface
     */
    public function getShard(?int $id, array $values = []): DatabaseInterface
    {
        $shard = $this->definition->getShard($id, $values);
        return $this->pool->db($shard);
    }

    /**
     * @param array $iterators
     *
     * @return Producer
     */
    private function joinIterators(array $iterators): Producer
    {
        $emit = function (callable $emit) use ($iterators) {
            $values = [];
            while ($iterators) {
                $keys = array_keys($iterators);
                foreach ($keys as $key) {
                    if (isset($values[$key])) {
                        continue;
                    }
                    if (yield $iterators[$key]->advance()) {
                        $values[$key] = $iterators[$key]->getCurrent();
                    } else {
                        unset($iterators[$key]);
                    }
                }
                uasort($values, function ($a, $b) {
                    return $a[0] - $b[0];
                });
                if (!$values) {
                    break;
                }
                $key = array_keys($values)[0];
                yield $emit($values[$key]);
                unset($values[$key]);
            }
        };
        return new Producer($emit);
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