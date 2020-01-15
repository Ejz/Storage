<?php

namespace Ejz;

class Type
{
    /**
     * @return AbstractType
     */
    public static function default(bool $nullable = false): AbstractType
    {
        return new Type\TypeDefault($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function string(bool $nullable = false): AbstractType
    {
        return new Type\TypeString($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function int(bool $nullable = false): AbstractType
    {
        return new Type\TypeInt($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function float(bool $nullable = false): AbstractType
    {
        return new Type\TypeFloat($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function bool(bool $nullable = false): AbstractType
    {
        return new Type\TypeBool($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function date(bool $nullable = false): AbstractType
    {
        return new Type\TypeDate($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function dateTime(bool $nullable = false): AbstractType
    {
        return new Type\TypeDateTime($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function json(bool $nullable = false): AbstractType
    {
        return new Type\TypeJson($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function bigInt(bool $nullable = false): AbstractType
    {
        return new Type\TypeBigInt($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function intArray(bool $nullable = false): AbstractType
    {
        return new Type\TypeIntArray($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function stringArray(bool $nullable = false): AbstractType
    {
        return new Type\TypeStringArray($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function binary(bool $nullable = false): AbstractType
    {
        return new Type\TypeBinary($nullable);
    }
}
