<?php

namespace Ejz;

class DatabasePool extends AbstractPool
{
    /**
     * @param mixed $object
     *
     * @return bool
     */
    protected function checkObject($object): bool
    {
        $_ = $object instanceof DatabaseInterface;
        return $_ && parent::checkObject($object);
    }

    // /**
    //  * @param string $notation
    //  * @param bool   $negate   (optional)
    //  *
    //  * @return Closure
    //  */
    // public static function convertNotationToFilter(string $notation, bool $negate = false): Closure
    // {
    //     return function ($name, $names) use ($notation, $negate) {
    //         $idx = array_search($name, $names);
    //         foreach (explode(',', $notation) as $n) {
    //             $neg = $negate;
    //             if (strpos($n, '!') === 0) {
    //                 $neg = !$neg;
    //                 $n = substr($n, 1);
    //             }
    //             $trig =
    //                 ($n === '*') ||
    //                 (is_numeric($n) && $n == $idx) ||
    //                 (!is_numeric($n) && $n === $name)
    //             ;
    //             if ($trig) {
    //                 return !$neg;
    //             }
    //             if  ($neg) {
    //                 return true;
    //             }
    //         }
    //         return false;
    //     };
    // }
}
