<?php

namespace Ejz;

class BitmapType
{
    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function default(bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable) extends AbstractType {
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function bool(bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable) extends AbstractType {
            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                if ($value === null) {
                    return $this->nullable ? null : false;
                }
                return (bool) $value;
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function string(bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable) extends AbstractType {
            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                if ($value === null) {
                    return $this->nullable ? null : '';
                }
                return (string) $value;
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function fulltext(bool $nullable = false): AbstractType
    {
        $type = self::string($nullable);
        $type->setName(__FUNCTION__);
        return $type;
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function triplets(bool $nullable = false): AbstractType
    {
        $type = self::string($nullable);
        $type->setName(__FUNCTION__);
        return $type;
    }

    /**
     * @param int  $min      (optional)
     * @param int  $max      (optional)
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function int(int $min = 0, int $max = 0, bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable, $min, $max) extends AbstractType {
            /** @var int */
            private $min;

            /** @var int */
            private $max;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param int    $min
             * @param int    $max
             */
            public function __construct(string $name, bool $nullable, int $min, int $max)
            {
                parent::__construct($name, $nullable);
                $min = $min < $max ? $min : $max;
                $this->min = $min;
                $this->max = $max;
            }

            /**
             * @return int
             */
            public function getMin(): int
            {
                return $this->min;
            }

            /**
             * @return int
             */
            public function getMax(): int
            {
                return $this->max;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                $min = $this->min;
                $max = $this->max;
                if ($value === null) {
                    return $this->nullable ? null : $min;
                }
                $value = (int) $value;
                return ($min <= $value && $value <= $max) ? $value : $min;
            }
        };
    }

    /**
     * @param string  $parent   (optional)
     * @param bool    $nullable (optional)
     *
     * @return AbstractType
     */
    public static function foreignKey(string $parent = '', bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable, $parent) extends AbstractType {
            /** @var string */
            private $parent;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param string $parent
             */
            public function __construct(string $name, bool $nullable, string $parent)
            {
                parent::__construct($name, $nullable);
                $this->parent = $parent;
            }

            /**
             * @return string
             */
            public function getParent(): string
            {
                return $this->parent;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                if ($value === null) {
                    return $this->nullable ? null : 0;
                }
                $value = (int) $value;
                return $value > 0 ? $value : 0;
            }
        };
    }

    /**
     * @param string  $separator (optional)
     * @param bool    $nullable  (optional)
     *
     * @return AbstractType
     */
    public static function array(string $separator = '|', bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable, $separator) extends AbstractType {
            /** @var string */
            private $separator;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param string $separator
             */
            public function __construct(string $name, bool $nullable, string $separator)
            {
                parent::__construct($name, $nullable);
                $this->separator = $separator;
            }

            /**
             * @return string
             */
            public function getSeparator(): string
            {
                return $this->separator;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                if ($value === null) {
                    return $this->nullable ? null : [];
                }
                return (array) $value;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function exportValue($value)
            {
                return implode($this->separator, $value);
            }
        };
    }

    /**
     * @param string $min      (optional)
     * @param string $max      (optional)
     * @param bool   $nullable (optional)
     *
     * @return AbstractType
     */
    public static function date(
        string $min = '2000-01-01',
        string $max = '2030-01-01',
        bool $nullable = false
    ): AbstractType {
        return new class(__FUNCTION__, $nullable, $min, $max) extends AbstractType {
            /** @var string */
            private $min;

            /** @var string */
            private $max;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param string $min
             * @param string $max
             */
            public function __construct(string $name, bool $nullable, string $min, string $max)
            {
                parent::__construct($name, $nullable);
                @ $min = (int) strtotime($min);
                @ $max = (int) strtotime($max);
                $min = $min < $max ? $min : $max;
                $min = date('Y-m-d', $min);
                $max = date('Y-m-d', $max);
                $this->min = $min;
                $this->max = $max;
            }

            /**
             * @return string
             */
            public function getMin(): string
            {
                return $this->min;
            }

            /**
             * @return string
             */
            public function getMax(): string
            {
                return $this->max;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                $min = $this->min;
                $max = $this->max;
                if ($value === null) {
                    return $this->nullable ? null : $min;
                }
                @ $value = (int) strtotime((string) $value);
                $value = date('Y-m-d', $value);
                return ($min <= $value && $value <= $max) ? $value : $min;
            }
        };
    }

    /**
     * @param string $min      (optional)
     * @param string $max      (optional)
     * @param bool   $nullable (optional)
     *
     * @return AbstractType
     */
    public static function dateTime(
        string $min = '2000-01-01 00:00:00',
        string $max = '2030-01-01 00:00:00',
        bool $nullable = false
    ): AbstractType {
        return new class(__FUNCTION__, $nullable, $min, $max) extends AbstractType {
            /** @var string */
            private $min;

            /** @var string */
            private $max;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param string $min
             * @param string $max
             */
            public function __construct(string $name, bool $nullable, string $min, string $max)
            {
                parent::__construct($name, $nullable);
                @ $min = (int) strtotime($min);
                @ $max = (int) strtotime($max);
                $min = $min < $max ? $min : $max;
                $min = date('Y-m-d H:i:s', $min);
                $max = date('Y-m-d H:i:s', $max);
                $this->min = $min;
                $this->max = $max;
            }

            /**
             * @return string
             */
            public function getMin(): string
            {
                return $this->min;
            }

            /**
             * @return string
             */
            public function getMax(): string
            {
                return $this->max;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                $min = $this->min;
                $max = $this->max;
                if ($value === null) {
                    return $this->nullable ? null : $min;
                }
                @ $value = (int) strtotime((string) $value);
                $value = date('Y-m-d H:i:s', $value);
                return ($min <= $value && $value <= $max) ? $value : $min;
            }
        };
    }
}
