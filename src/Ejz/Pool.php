<?php

namespace Ejz;

class Pool
{
    /** @var array */
    private $pool;

    public const POOL_WRITABLE = 6;
    public const POOL_PRIMARY = 2;
    public const POOL_SECONDARY = 4;
    public const POOL_RANDOM_GET = 1;
    public const POOL_RANDOM = 1;

    public const POOL_CLUSTER_DEFAULT_W = 'w:*;';
    public const POOL_CLUSTER_DEFAULT_P = 'p:*;';
    public const POOL_CLUSTER_DEFAULT_S = 's:1:id;';

    /**
     * @param array $pool
     */
    public function __construct(array $pool)
    {
        $this->pool = [];
        foreach ($pool as $instance) {
            $this->pool[$instance->getName()] = $instance;
        }
    }

    /**
     * @param string $name
     *
     * @return ?object
     */
    public function instance(string $name): ?object
    {
        return $this->pool[$name] ?? null;
    }

    /**
     * @param mixed $filter
     *
     * @return self
     */
    public function filter($filter): self
    {
        $names = $this->names();
        $pool = [];
        $is_callable = is_callable($filter);
        foreach ($this->pool as $name => $instance) {
            if (
                ($is_callable && $filter($name, $names)) ||
                in_array($name, (array) $filter)
            ) {
                $pool[] = $instance;
            }
        }
        return new self($pool);
    }

    /**
     * @return ?object
     */
    public function random(): ?object
    {
        @ $key = array_rand($this->pool);
        return $key === null ? null : $this->pool[$key];
    }

    /**
     * @return array
     */
    public function names(): array
    {
        return array_keys($this->pool);
    }

    /**
     * @return int
     */
    public function size(): int
    {
        return count($this->pool);
    }

    /**
     * @param callable $function
     *
     * @return array
     */
    public function each(callable $function): array
    {
        return array_map(function ($instance) use ($function) {
            return $function($instance);
        }, $this->pool);
    }

    /**
     * @param string $call
     * @param array  $arguments
     *
     * @return array
     */
    public function __call(string $call, array $arguments): array
    {
        return $this->each(function ($instance) use ($call, $arguments) {
            return $instance->$call(...$arguments);
        });
    }

    
}
