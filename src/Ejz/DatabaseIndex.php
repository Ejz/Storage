<?php

namespace Ejz;

class DatabaseIndex
{
    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function BTREE(...$fields): array
    {
        return [
            'fields' => $fields,
            'type' => __FUNCTION__,
        ];
    }

    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function HASH(...$fields): array
    {
        return [
            'fields' => $fields,
            'type' => __FUNCTION__,
        ];
    }

    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function GIST(...$fields): array
    {
        return [
            'fields' => $fields,
            'type' => __FUNCTION__,
        ];
    }

    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function GIN(...$fields): array
    {
        return [
            'fields' => $fields,
            'type' => __FUNCTION__,
        ];
    }

    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function UNIQUE(...$fields): array
    {
        return [
            'fields' => $fields,
            'type' => __FUNCTION__,
            'databaseIndex' => 'BTREE',
        ];
    }
}
