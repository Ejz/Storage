<?php

namespace Ejz\Type;

use RuntimeException;

class TypeEnum extends AbstractType
{
    /** @var array */
    private $enums;

    private const ERROR_INVALID_ENUMS = 'ERROR_INVALID_ENUMS';

    /**
     * @param array $enums
     */
    public function __construct(array $enums)
    {
        $this->nullable = false;
        $enums = array_map('strval', $enums);
        $enums = array_unique($enums);
        $enums = array_values($enums);
        $this->enums = $enums;
    }

    /**
     * @return string
     */
    public function getDefault(): string
    {
        $default = $this->enums[0] ?? null;
        if ($default === null) {
            $message = self::ERROR_INVALID_ENUMS;
            throw new RuntimeException($message);
        }
        return $default;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        @ $value = (string) $value;
        $value = in_array($value, $this->enums) ? $value : $this->getDefault();
        return $value;
    }
}
