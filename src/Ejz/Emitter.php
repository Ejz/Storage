<?php

namespace Ejz;

use Generator;
use Amp\Promise;
use Amp\Success;

class Emitter
{
    private const EMITTER_IS_FINISHED = 'EMITTER_IS_FINISHED';
    private $push = [];
    private $pull = [];
    private $values = [];

    private $pos0 = 0;
    private $pos1 = 0;

    private $finished = false;

    private $stats = ['push' => 0, 'pull' => 0];

    public function push($value): Promise
    {
        if ($this->pull) {
            $key = array_rand($this->pull);
            $deferred = $this->pull[$key];
            unset($this->pull[$key]);
            $deferred->resolve($value);
            $this->stats['push']++;
            return new Success();
        }
        if ($this->finished) {
            throw new RuntimeException(self::EMITTER_IS_FINISHED);
        }
        $this->stats['push']++;
        $this->values[$this->pos0] = $value;
        $this->pos0++;
        $deferred = new \Amp\Deferred();
        $this->push[] = $deferred;
        return $deferred->promise();
    }

    public function pull(): Promise
    {
        if (array_key_exists($this->pos1, $this->values)) {
            $key = array_rand($this->push);
            $deferred = $this->push[$key];
            unset($this->push[$key]);
            $deferred->resolve();
            $value = $this->values[$this->pos1];
            unset($this->values[$this->pos1]);
            $this->pos1++;
            $this->stats['pull']++;
            return new Success($value);
        }
        if ($this->finished) {
            return new Success(null);
        }
        $this->stats['pull']++;
        $deferred = new \Amp\Deferred();
        $this->pull[] = $deferred;
        return $deferred->promise();
    }

    public function finish()
    {
        $this->finished = true;
        foreach ($this->push as $deferred) {
            $deferred->resolve();
        }
        $this->push = [];
        foreach ($this->pull as $deferred) {
            $deferred->resolve(null);
        }
        $this->pull = [];
    }

    public function resetStats()
    {
        $this->stats = ['push' => 0, 'pull' => 0];
    }

    // public function advance(): Promise
    // {
    //     if ($this->finished) {
    //         return new \Amp\Success(false);
    //     }
    //     $deferred = new \Amp\Deferred();
    //     $this->advances[] = $deferred;
    //     return $deferred->promise();
    //     // if ($this->advancePromise !== null) {
    //     //     return $this->advancePromise->promise();
    //     // }
    //     if (($_ = $this->emitPromise) !== null) {
    //         $this->emitPromise = null;
    //         $_->resolve();
    //         // \Amp\Loop::defer(function () use ($_) {
    //         // });
    //     }
    //     $this->advancePromise = new \Amp\Deferred();
    //     return $this->advancePromise->promise();
    // }

    

    // public function getCurrent()
    // {
    //     if ($this->isFinished) {
    //         throw new \RuntimeException("The iterator has completed");
    //     }
    //     if (!array_key_exists($this->position, $this->values)) {
    //         throw new \RuntimeException("advance");
    //     }
    //     return $this->currentValue;
    // }

    /** @var Iterator */
    private $iterator;

    /** @var int */
    private $size;

    /** @var string */
    private $cursor;

    /** @var mixed */
    private $context;

    /**
     * @param Iterator $iterator
     */
    public function __construct(Iterator $iterator = null)
    {
        $this->iterator = $iterator;
    }

    /**
     * @return bool
     */
    public function advanceSync(): bool
    {
        return Promise\wait($this->iterator->advance());
    }

    /**
     * @return mixed
     */
    public function pullSync()
    {
        return Promise\wait($this->pull());
    }

    /**
     * @return Promise
     */
    public function advance1(): Promise
    {
        return $this->iterator->advance();
    }

    /**
     * @return mixed
     */
    public function getCurrent1()
    {
        return $this->iterator->getCurrent();
    }

    /**
     * @return int
     */
    public function getSize(): int
    {
        return $this->size;
    }

    /**
     * @param int $size
     */
    public function setSize(int $size)
    {
        $this->size = $size;
    }

    /**
     * @return string
     */
    public function getCursor(): string
    {
        return $this->cursor;
    }

    /**
     * @param string $cursor
     */
    public function setCursor(string $cursor)
    {
        $this->cursor = $cursor;
    }

    /**
     * @return mixed
     */
    public function getContext()
    {
        return $this->context;
    }

    /**
     * @param mixed $context
     */
    public function setContext($context, $key = null)
    {
        if ($key === null) {
            $this->context = $context;
        } else {
            $this->context[$key] = $context;
        }
    }

    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        while (($value = $this->pullSync()) !== null) {
            yield $value[0] => $value[1];
        }
    }

    /**
     * @return array
     */
    public function getSearchState(): array
    {
        $ctx = $this->context;
        if (isset($ctx['cursor'])) {
            $ids = $ctx['ids'];
            $ids = array_keys(array_filter($ids, 'is_int'));
            $ctx['ids'] = $ids;
            return $ctx + ['size' => $this->size];
        }
        $collect = [];
        $ids = $ctx['ids'] ?? [];
        foreach (($ctx['emitters'] ?? []) as $name => $emitter) {
            $state = $emitter->getSearchState();
            if (isset($ids[$name])) {
                array_unshift($state['ids'], $ids[$name]);
            }
            $collect[$name] = $state;
        }
        return $collect;
    }

    /**
     * @param Emitter $iterator
     * @param array   $ids
     *
     * @return Emitter
     */
    public static function getIteratorWithIdsOrder(self $iterator, array $ids): self
    {
        $emitter = new self();
        $collect = [];
        $c1 = \Amp\call(function () use (&$collect, &$iterator) {
            while (($value = yield $iterator->pull()) !== null) {
                [$id, $bean] = $value;
                $collect[$id] = $bean;
            }
            $iterator = null;
        });
        $c2 = \Amp\call(function ($ids, $emitter) use (&$collect, &$iterator) {
            $pointer = 0;
            $count = count($ids);
            while ($iterator !== null && $pointer < $count) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->push([$id, $collect[$id]]);
                    $pointer++;
                } else {
                    yield \Amp\delay(20);
                }
            }
            for (; $pointer < $count; $pointer++) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->push([$id, $collect[$id]]);
                }
            }
        }, $ids, $emitter);
        Promise\all([$c1, $c2])->onResolve(function () use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
    }

    public static function fromIterable(iterable $iterable): self
    {
        $emitter = new self();
        $coroutine = \Amp\call(function ($iterable, $emitter) {
            foreach ($iterable as $value) {
                yield $emitter->push($value);
            }
        }, $iterable, $emitter);
        $coroutine->onResolve(function () use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
    }

    
    public static function merge(array $iterators): self
    {
        $emitter = new self();
        $coroutines = [];
        foreach ($iterators as $iterator) {
            $coroutines[] = \Amp\call(function ($iterator, $emitter) {
                while (($value = yield $iterator->pull()) !== null) {
                    yield $emitter->push($value);
                }
            }, $iterator, $emitter);
        }
        Promise\all($coroutines)->onResolve(function () use ($emitter) {
            $emitter->finish();
        });
        return $emitter;
    }
}

