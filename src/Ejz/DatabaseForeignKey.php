<?php

namespace Ejz;

class DatabaseForeignKey
{
    /**
     * @param array ...$fields
     *
     * @return array
     */
    public static function get(...$fields): array
    {
        $c = count($fields);
        if ($c === 1) {
            [$t, $f] = explode('.', $fields[0]);
            $fields = [$f, $t, $f];
            $c = 3;
        }
        return [
            'childFields' => array_slice($fields, 0, ($c - 1) / 2),
            'parentTable' => $fields[($c - 1) / 2],
            'parentFields' => array_slice($fields, (($c - 1) / 2) + 1),
        ];
    }
}
