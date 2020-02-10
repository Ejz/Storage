<?php

namespace Ejz;

use Amp\Promise;

class Iterator implements \Amp\Iterator, \Iterator
{
    use SyncTrait;

    /** @var \Amp\Iterator */
    private $iterator;

    /** @var ?bool */
    private $next;

    /**
     * @param mixed $iterator
     */
    public function __construct($iterator)
    {
        if (!is_callable($iterator)) {
            $_ = $iterator;
            $iterator = function ($emit) use ($_) {
                foreach ($_ as $value) {
                    yield $emit($value);
                }
            };
        }
        $this->iterator = new \Amp\Producer($iterator);
    }

    /**
     * @return Promise
     */
    public function advance(): Promise
    {
        return $this->iterator->advance();
    }

    /**
     * @return mixed
     */
    public function getCurrent()
    {
        return $this->iterator->getCurrent();
    }

    /**
     * @return mixed
     */
    public function current()
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true ? $this->iterator->getCurrent()[1] : null;
    }

    /**
     * @return int
     */
    public function key(): ?int
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true ? $this->iterator->getCurrent()[0] : null;
    }

    /**
     */
    public function next()
    {
        $this->next = $this->advanceSync();
    }

    /**
     */
    public function valid(): bool
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true;
    }

    /**
     */
    public function rewind()
    {
    }

    /**
     * @param array     $iterators
     * @param ?callable $sort      (optional)
     *
     * @return self
     */
    public static function merge(array $iterators, ?callable $sort = null): self
    {
        $emit = function ($emit) use ($iterators, $sort) {
            $values = [];
            $ids = [];
            while (true) {
                do {
                    $diff = array_diff_key($iterators, $values);
                    $results = yield array_map(function ($iterator) {
                        return $iterator->advance();
                    }, $diff);
                    foreach ($results as $key => $value) {
                        if ($value === false) {
                            unset($iterators[$key]);
                            unset($diff[$key]);
                            continue;
                        }
                        $value = $iterators[$key]->getCurrent();
                        if (!isset($ids[$value[0]])) {
                            $ids[$value[0]] = true;
                            $values[$key] = $value;
                            unset($diff[$key]);
                        }
                    }
                } while ($diff);
                if (!$values) {
                    break;
                }
                if ($sort !== null) {
                    uasort($values, $sort);
                    $key = key($values);
                } else {
                    $key = array_rand($values);
                }
                yield $emit($values[$key]);
                unset($values[$key]);
            }
        };
        return new self($emit);
    }
}
