<?php

namespace Ejz;

trait ContextTrait
{
    /** @var array */
    private $context;

    /**
     * @return array
     */
    public function getContext(): array
    {
        return $this->context;
    }

    /**
     * @param mixed   $value
     * @param ?string $key   (optional)
     */
    public function setContext($value, ?string $key = null)
    {
        if ($key === null) {
            $this->context = $value;
        } else {
            $this->context[$key] = $value;
        }
    }
}