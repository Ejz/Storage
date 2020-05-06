<?php

namespace Ejz;

class DatabaseType
{
    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function DEFAULT(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                return $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function STRING(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'TEXT',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : '';
                }
                return (string) $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function INTEGER(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value instanceof AbstractBean) {
                    return $value;
                }
                if ($value === null) {
                    return $this->isNullable() ? null : 0;
                }
                return (int) $value;
            }

            public function exportValue($value)
            {
                return $value instanceof AbstractBean ? $value->id : $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function FLOAT(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'REAL',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : 0.0;
                }
                return (float) $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function BOOLEAN(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : false;
                }
                return (bool) $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function DATE(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : date('Y-m-d', 0);
                }
                return date('Y-m-d', strtotime($value));
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function DATETIME(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'TIMESTAMP(0) WITHOUT TIME ZONE',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : date('Y-m-d H:i:s', 0);
                }
                return date('Y-m-d H:i:s', strtotime($value));
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function JSON(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'JSONB',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : [];
                }
                return (array) $value;
            }

            public function importValue($value)
            {
                $value = $value !== null ? json_decode($value, true) : null;
                return $this->setValue($value);
            }

            public function exportValue($value)
            {
                return $value !== null ? json_encode($value) : null;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function BIGINT(array $options = []): AbstractType
    {
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value instanceof AbstractBean) {
                    return $value;
                }
                if ($value === null) {
                    return $this->isNullable() ? null : 0;
                }
                return (int) $value;
            }

            public function exportValue($value)
            {
                return $value instanceof AbstractBean ? $value->id : $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function INTARRAY(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'INTEGER[]',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : [];
                }
                return array_map('intval', (array) $value);
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function STRARRAY(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'TEXT[]',
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : [];
                }
                return array_map('strval', (array) $value);
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function ENUM(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'TEXT',
            'enums' => [],
        ];
        $options['enums'] = array_map('strval', (array) $options['enums']);
        $options['enums'] = array_unique($options['enums']);
        $options['enums'] = array_values($options['enums']);
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                $enums = $this->options['enums'];
                if ($value === null) {
                    return $this->isNullable() ? null : $enums[0];
                }
                $value = (string) $value;
                return in_array($value, $enums, true) ? $value : $enums[0];
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function BINARY(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'BYTEA',
            'binary' => true,
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : '';
                }
                return (string) $value;
            }
        };
    }

    /**
     * @param array $options (optional)
     *
     * @return AbstractType
     */
    public static function COMPRESSED(array $options = []): AbstractType
    {
        $options += [
            'databaseType' => 'BYTEA',
            'binary' => true,
        ];
        return new class(__FUNCTION__, $options) extends AbstractType {
            public function setValue($value)
            {
                if ($value === null) {
                    return $this->isNullable() ? null : '';
                }
                return (string) $value;
            }

            public function importValue($value)
            {
                $value = $value !== null ? gzinflate($value) : null;
                return $this->setValue($value);
            }

            public function exportValue($value)
            {
                return $value !== null ? gzdeflate($value) : null;
            }
        };
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return AbstractType
     */
    public static function __callStatic(string $name, array $arguments): AbstractType
    {
        $name = strtoupper($name);
        $map = [
            'INT' => 'INTEGER',
            'BOOL' => 'BOOLEAN',
            'COMPRESSEDBINARY' => 'COMPRESSED',
            'REAL' => 'FLOAT',
            'TEXT' => 'STRING',
            'STRINGARRAY' => 'STRARRAY',
            'INTEGERARRAY' => 'INTARRAY',
        ];
        $map = $map[$name];
        return self::$map(...$arguments);
    }
}
