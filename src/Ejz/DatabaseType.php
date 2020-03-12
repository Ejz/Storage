<?php

namespace Ejz;

class DatabaseType
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
    public static function int(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : 0;
                }
                return (int) $value;
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function float(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : 0.0;
                }
                return (float) $value;
            }
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
    public static function date(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : date('Y-m-d', 0);
                }
                return date('Y-m-d', strtotime($value));
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function dateTime(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : date('Y-m-d H:i:s', 0);
                }
                return date('Y-m-d H:i:s', strtotime($value));
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function json(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : [];
                }
                return (array) $value;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function importValue($value)
            {
                $value = $value !== null ? json_decode($value, true) : null;
                return $this->hydrateValue($value);
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function exportValue($value)
            {
                return $value !== null ? json_encode($value) : null;
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function bigInt(bool $nullable = false): AbstractType
    {
        $type = self::int($nullable);
        $type->setName(__FUNCTION__);
        return $type;
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function intArray(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : [];
                }
                return array_map('intval', (array) $value);
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function stringArray(bool $nullable = false): AbstractType
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
                    return $this->nullable ? null : [];
                }
                return array_map('strval', (array) $value);
            }
        };
    }

    /**
     * @param array $enums    (optional)
     * @param bool  $nullable (optional)
     *
     * @return AbstractType
     */
    public static function enum(array $enums = [], bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable, $enums) extends AbstractType {
            /** @var array */
            private $enums;

            /**
             * @param string $name
             * @param bool   $nullable
             * @param array  $enums
             */
            public function __construct(string $name, bool $nullable, array $enums)
            {
                parent::__construct($name, $nullable);
                $enums = array_map('strval', $enums);
                $enums = array_unique($enums);
                $enums = array_values($enums);
                $this->enums = $enums;
            }

            /**
             * @return array
             */
            public function getEnums(): array
            {
                return $this->enums;
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function hydrateValue($value)
            {
                if ($value !== null) {
                    $value = (string) $value;
                    if (in_array($value, $this->enums, true)) {
                        return $value;
                    }
                }
                return $this->nullable ? null : $this->enums[0];
            }
        };
    }

    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function binary(bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable) extends AbstractType {
            /**
             * @param string $name
             * @param bool   $nullable
             */
            public function __construct(string $name, bool $nullable)
            {
                parent::__construct($name, $nullable);
                $this->binary = true;
            }

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
    public static function compressedBinary(bool $nullable = false): AbstractType
    {
        return new class(__FUNCTION__, $nullable) extends AbstractType {
            /**
             * @param string $name
             * @param bool   $nullable
             */
            public function __construct(string $name, bool $nullable)
            {
                parent::__construct($name, $nullable);
                $this->binary = true;
            }

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

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function importValue($value)
            {
                $value = $value !== null ? gzinflate($value) : null;
                return $this->hydrateValue($value);
            }

            /**
             * @param mixed $value
             *
             * @return mixed
             */
            public function exportValue($value)
            {
                return $value !== null ? gzdeflate($value) : null;
            }
        };
    }
}
