<?php

namespace Ejz;

use RuntimeException;

class AbstractPool implements PoolInterface
{
    /** @var array */
    protected $pool;

    /** @var string */
    private const ERROR_INVALID_OBJECT = 'ERROR_INVALID_OBJECT';

    /**
     * @param array $pool
     */
    public function __construct(array $pool)
    {
        $this->pool = [];
        foreach ($pool as $object) {
            if (!$this->checkObject($object)) {
                throw new RuntimeException(self::ERROR_INVALID_OBJECT);
            }
            $this->pool[$object->getName()] = $object;
        }
    }

    /**
     * @param mixed $object
     *
     * @return bool
     */
    protected function checkObject($object): bool
    {
        return $object instanceof NameInterface;
    }

    /**
     * @param string $name
     *
     * @return ?NameInterface
     */
    public function get(string $name): ?NameInterface
    {
        return $this->pool[$name] ?? null;
    }

    /**
     * @return ?NameInterface
     */
    public function random(): ?NameInterface
    {
        @ $key = array_rand($this->pool);
        return $key === null ? null : $this->pool[$key];
    }

    /**
     * @param mixed $filter
     *
     * @return PoolInterface
     */
    public function filter($filter): PoolInterface
    {
        $names = array_keys($this->pool);
        $pool = [];
        $is_callable = is_callable($filter);
        foreach ($this->pool as $name => $object) {
            if (
                ($is_callable && $filter($name, $names)) ||
                in_array($name, (array) $filter)
            ) {
                $pool[] = $object;
            }
        }
        return new static($pool);
    }

    /**
     * @return int
     */
    public function count(): int
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
        return array_map(function ($object) use ($function) {
            return $function($object);
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
        return $this->each(function ($object) use ($call, $arguments) {
            return $object->$call(...$arguments);
        });
    }

    /**
     * @deprecated
     */
    public function names()
    {
        return array_keys($this->pool);
    }

    /**
     * @deprecated
     */
    public function size(): int
    {
        return $this->count();
    }
}
