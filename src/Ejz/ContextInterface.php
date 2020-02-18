<?php

namespace Ejz;

interface ContextInterface
{
    /**
     * @return array
     */
    public function getContext(): array;

    /**
     * @param mixed   $value
     * @param ?string $key   (optional)
     */
    public function setContext($value, ?string $key = null);
}