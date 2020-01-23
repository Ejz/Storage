<?php

namespace Ejz;

class Type
{
    /**
     * @return Type\AbstractType
     */
    public static function default(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeDefault($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function string(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeString($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function int(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeInt($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function float(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeFloat($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function bool(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeBool($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function date(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeDate($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function dateTime(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeDateTime($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function json(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeJson($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function bigInt(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeBigInt($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function intArray(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeIntArray($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function stringArray(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeStringArray($nullable);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function binary(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeBinary($nullable);
    }

    /**
     * @param array $enums (optional)
     *
     * @return Type\AbstractType
     */
    public static function enum(array $enums = []): Type\AbstractType
    {
        return new Type\TypeEnum($enums);
    }

    /**
     *
     * @return Type\AbstractType
     */
    public static function bitmapBool(): Type\AbstractType
    {
        return new Type\TypeBitmapBool();
    }

    /**
     * @param string $table (optional)
     *
     * @return Type\AbstractType
     */
    public static function bitmapForeignKey(string $table = ''): Type\AbstractType
    {
        return new Type\TypeBitmapForeignKey($table);
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return Type\AbstractType
     */
    public static function compressedBinary(bool $nullable = false): Type\AbstractType
    {
        return new Type\TypeCompressedBinary($nullable);
    }
}
