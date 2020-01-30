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

    public function push($value): Promise
    {
        if ($this->pull) {
            $key = array_rand($this->pull);
            $deferred = $this->pull[$key];
            unset($this->pull[$key]);
            $deferred->resolve($value);
            return new Success();
        }
        if ($this->finished) {
            throw new RuntimeException(self::EMITTER_IS_FINISHED);
        }
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
            return new Success($value);
        }
        if ($this->finished) {
            return new Success(null);
        }
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
    public function setContext($context)
    {
        $this->context = $context;
    }

    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        while (($value = Promise\wait($this->pull())) !== null) {
            yield $value[0] => $value[1];
        }
    }

    /**
     * @return array
     */
    public function getSearchIteratorState(): array
    {
        if ($this->context instanceof Bitmap) {
            $state = $this->context->getSearchIteratorState($this->cursor);
            return ['size' => $this->size, 'cursor' => $this->cursor] + $state;
        }
        [
            'repository' => $repository,
            'iterators' => $iterators,
        ] = $this->context;
        $ids = $repository->getSearchIteratorState($this->cursor);
        $collect = [];
        foreach ($iterators as $name => $iterator) {
            $state = $iterator->getSearchIteratorState();
            if (isset($ids[$name])) {
                $_pointer = $state['pointer'] ?? 0;
                $_ids = $state['ids'] ?? [];
                if ($_pointer > 0) {
                    $_ids = array_slice($_ids, $_pointer);
                }
                array_unshift($_ids, $ids[$name]);
                $state['ids'] = $_ids;
                $state['pointer'] = 0;
            }
            $collect[$name] = $state;
        }
        return $collect;
    }

    /**
     * @param Iterator $iterator
     * @param array    $ids
     *
     * @return Iterator
     */
    public static function getIteratorWithIdsOrder(Iterator $iterator, array $ids): Iterator
    {
        $emitter = new \Amp\Emitter();
        $return = $emitter->iterate();
        $return = new self($return);
        $collect = [];
        $c1 = \Amp\call(function () use (&$collect, &$iterator, &$emitter) {
            while ((yield $iterator->advance()) && $emitter !== null) {
                [$id, $bean] = $iterator->getCurrent();
                $collect[$id] = $bean;
            }
            $iterator = null;
        });
        $c2 = \Amp\call(function ($ids) use (&$collect, &$iterator, &$emitter) {
            $pointer = 0;
            $count = count($ids);
            while ($iterator !== null && $emitter !== null && $pointer < $count) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->emit([$id, $collect[$id]]);
                    $pointer++;
                } else {
                    yield \Amp\delay(20);
                }
            }
            for (; $emitter !== null && $pointer < $count; $pointer++) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->emit([$id, $collect[$id]]);
                }
            }
        }, $ids);
        Promise\all([$c1, $c2])->onResolve(function ($exception) use (&$emitter) {
            if ($exception) {
                $emitter->fail($exception);
                $emitter = null;
            } else {
                $emitter->complete();
            }
        });
        return $return;
    }
}

