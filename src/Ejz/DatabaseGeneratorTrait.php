<?php

namespace Ejz;

use Generator;

trait DatabaseGeneratorTrait
{
    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        $iterator = function () {
            if (yield $this->advance()) {
                return $this->getCurrent();
            }
        };
        while ($yield = \Amp\Promise\wait(\Amp\call($iterator, $producer))) {
            yield $yield[0] => $yield[1];
        }
    }
}
