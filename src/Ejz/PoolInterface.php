<?php

namespace Ejz;

use Countable;

interface PoolInterface extends Countable
{
    /**
     * @param string $name
     *
     * @return ?NameInterface
     */
    public function get(string $name): ?NameInterface;

    /**
     * @return ?NameInterface
     */
    public function random(): ?NameInterface;

    /**
     * @param mixed $filter
     *
     * @return self
     */
    public function filter($filter): self;

    /**
     * @param callable $function
     *
     * @return array
     */
    public function each(callable $function): array;
}