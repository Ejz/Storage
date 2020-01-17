<?php

namespace Ejz;

use Generator;

class Producer implements \Amp\Iterator
{
    use \Amp\CallableMaker;
    use \Amp\Internal\Producer;

    /**
     * @param callable $producer
     */
    public function __construct(callable $producer)
    {
        $result = $producer($this->callableFromInstanceMethod('emit'));
        $coroutine = new \Amp\Coroutine($result);
        $coroutine->onResolve(function ($exception) {
            if ($this->complete) {
                return;
            }
            if ($exception) {
                $this->fail($exception);
                return;
            }
            $this->complete();
        });
    }

    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while (($yield = \Amp\Promise\wait(\Amp\call($iterator, $this))) !== null) {
            yield $yield[0] => $yield[1];
        }
    }
}

