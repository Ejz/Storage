<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeBinary extends AbstractType
{
	/**
     * @return string
     */
    public function getSelectString(): string
    {
        return 'encode((%s)::BYTEA, \'base64\')';
    }

    /**
     * @return string
     */
    public function getInsertString(): string
    {
        return 'decode((?)::TEXT, \'base64\')';
    }

    /**
     * @return string
     */
    public function getUpdateString(): string
    {
        return 'decode((?)::TEXT, \'base64\')';
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function import($value)
    {
        return base64_decode($value);
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        return base64_encode($value);
    }
}
