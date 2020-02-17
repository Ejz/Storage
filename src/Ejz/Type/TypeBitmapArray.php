<?php

namespace Ejz\Type;

class TypeBitmapArray extends AbstractType
{
    /** @var string */
    private $separator;

    /**
     * @param string $separator
     */
    public function __construct(string $separator)
    {
        parent::__construct(true);
        $this->separator = $separator;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        return $value === null ? null : implode('|', (array) $value);
    }

    /**
     * @return string
     */
    public function getSeparator(): string
    {
        return $this->separator;
    }
}
