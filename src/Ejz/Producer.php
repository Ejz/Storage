<?php

namespace Ejz;

abstract class Producer implements \Amp\Iterator
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
}

