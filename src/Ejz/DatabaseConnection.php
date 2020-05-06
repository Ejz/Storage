<?php

namespace Ejz;

use Amp\Loop;
use Amp\Promise;
use Amp\Failure;
use Amp\Deferred;

class DatabaseConnection
{
    /** @var ?resource */
    private $connection;

    /** @var ?Deferred */
    private $deferred;

    /** @var string */
    private $poll;

    /** @var string */
    private const CONNECTION_ERROR = 'CONNECTION_ERROR';

    /** @var string */
    private const DEFERRED_ERROR = 'DEFERRED_ERROR';

    /** @var string */
    private const SOCKET_ERROR = 'SOCKET_ERROR';

    /** @var string */
    private const DISCONNECT_ERROR = 'DISCONNECT_ERROR';

    /** @var string */
    private const EMPTY_QUERY_ERROR = 'EMPTY_QUERY_ERROR';

    /** @var string */
    private const UNKNOWN_RESULT_ERROR = 'UNKNOWN_RESULT_ERROR';

    /**
     * @param resource $connection
     * @param resource $socket
     */
    public function __construct($connection, $socket)
    {
        $this->connection = $connection;
        $connection = &$this->connection;
        $deferred = &$this->deferred;
        $poll = static function ($watcher) use (&$deferred, &$connection) {
            if (!\pg_consume_input($connection)) {
                $connection = null;
                Loop::disable($watcher);
                if ($deferred !== null) {
                    $deferred->fail(new DatabaseException(\pg_last_error()));
                }
                return;
            }
            if ($deferred === null) {
                return;
            }
            if (\pg_connection_busy($connection)) {
                return;
            }
            $deferred->resolve(\pg_get_result($connection));
            if ($deferred === null) {
                Loop::disable($watcher);
            }
        };
        $this->poll = Loop::onReadable($socket, $poll);
        Loop::disable($this->poll);
    }

    /**
     * @param string $dsn
     *
     * @return Promise
     */
    public static function connect(string $dsn): Promise
    {
        if (!$connection = @ \pg_connect($dsn, \PGSQL_CONNECT_ASYNC | \PGSQL_CONNECT_FORCE_NEW)) {
            return new Failure(new DatabaseException(self::CONNECTION_ERROR));
        }
        if (\pg_connection_status($connection) === \PGSQL_CONNECTION_BAD) {
            return new Failure(new DatabaseException(\pg_last_error($connection)));
        }
        if (!$socket = \pg_socket($connection)) {
            return new Failure(new DatabaseException(self::SOCKET_ERROR));
        }
        $deferred = new Deferred();
        $promise = $deferred->promise();
        $callback = function ($watcher, $socket) use ($connection, $deferred) {
            switch (\pg_connect_poll($connection)) {
                case \PGSQL_POLLING_READING:
                case \PGSQL_POLLING_WRITING:
                    return;

                case \PGSQL_POLLING_FAILED:
                    $deferred->fail(new DatabaseException(\pg_last_error($connection)));
                    return;

                case \PGSQL_POLLING_OK:
                    $deferred->resolve(new self($connection, $socket));
                    return;
            }
        };
        $poll = Loop::onReadable($socket, $callback);
        $await = Loop::onWritable($socket, $callback);
        $promise->onResolve(function ($exception) use ($connection, $poll, $await) {
            if ($exception) {
                \pg_close($connection);
            }
            Loop::cancel($poll);
            Loop::cancel($await);
        });
        return $promise;
    }

    /**
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     */
    public function close()
    {
        if ($this->deferred !== null) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->fail(new DatabaseException(self::DEFERRED_ERROR));
        }
        if ($this->connection !== null) {
            \pg_close($this->connection);
        }
        Loop::cancel($this->poll);
        $this->connection = null;
    }

    /**
     * @param string $sql
     * @param array  $args (optional)
     *
     * @return Promise
     */
    public function query(string $sql, array $args = []): Promise
    {
        if ($this->connection === null) {
            throw new DatabasePostgresConnection();
        }
        return \Amp\call(function ($sql, $args) {
            $sql = $this->substitute($sql, $args);
            return $this->createResult(yield from $this->send('pg_send_query', $sql));
        }, $sql, $args);
    }

    /**
     * @param callable $function
     * @param array    ...$args
     *
     * @return \Generator
     */
    private function send(callable $function, ...$args): \Generator
    {
        while ($this->deferred !== null) {
            try {
                yield $this->deferred->promise();
            } catch (\Throwable $exception) {
            }
        }
        if ($this->connection === null) {
            throw new DatabaseException(self::DISCONNECT_ERROR);
        }
        $result = $function($this->connection, ...$args);
        if ($result === false) {
            throw new DatabaseException(\pg_last_error());
        }
        $this->deferred = new Deferred();
        Loop::enable($this->poll);
        try {
            $result = yield $this->deferred->promise();
        } finally {
            $this->deferred = null;
        }
        return $result;
    }

    /**
     * @param resource $result
     *
     * @return mixed
     */
    private function createResult($result)
    {
        switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
            case \PGSQL_COMMAND_OK:
                return \pg_affected_rows($result);

            case \PGSQL_TUPLES_OK:
                $types = $this->getResultTypes($result);
                $return = [];
                while (($array = \pg_fetch_array($result, null, \PGSQL_ASSOC)) !== false) {
                    $pos = 0;
                    $array = array_map(function ($value) use (&$pos, &$types) {
                        $type = $types[$pos++];
                        return $value === null ? null : $this->castFrom($value, $type);
                    }, $array);
                    $return[] = $array;
                }
                return $return;

            case \PGSQL_NONFATAL_ERROR:
            case \PGSQL_FATAL_ERROR:
            case \PGSQL_BAD_RESPONSE:
                throw new DatabaseException(\pg_result_error($result));

            case \PGSQL_EMPTY_QUERY:
                throw new DatabaseException(self::EMPTY_QUERY_ERROR);

            default:
                throw new DatabaseException(self::UNKNOWN_RESULT_ERROR);
        }
    }

    /**
     * @param string $sql
     * @param array  $args
     *
     * @return string
     */
    private function substitute(string $sql, array $args): string
    {
        $sql = trim($sql);
        $sql = preg_replace('~\s+~', ' ', $sql);
        $args = array_values($args);
        $pos = 0;
        $sql = preg_replace_callback('~((\\?|#|%|\\$)+)~i', function ($match) use (&$pos, $args) {
            $len = strlen($match[0]);
            $single = $match[0][0];
            $return = str_repeat($single, (int) ($len / 2));
            if ($len % 2 === 1) {
                $value = $args[$pos++] ?? null;
                if ($single === '?') {
                    $value = $this->castTo($value);
                } elseif ($single === '#') {
                    $value = $this->quoteName($value);
                } elseif ($single === '$') {
                    $value = $value === null ? 'NULL' : '\'' . $this->quoteBinary($value) . '\'';
                }
                $return .= $value;
            }
            return $return;
        }, $sql);
        return trim($sql);
    }

    /**
     * @param string $value
     *
     * @return string
     */
    public function quoteValue(string $value): string
    {
        if ($this->connection === null) {
            throw new DatabaseException(self::DISCONNECT_ERROR);
        }
        return \pg_escape_literal($this->connection, $value);
    }

    /**
     * @param string $value
     *
     * @return string
     */
    public function quoteName(string $name): string
    {
        if ($this->connection === null) {
            throw new DatabaseException(self::DISCONNECT_ERROR);
        }
        return \pg_escape_identifier($this->connection, $name);
    }

    /**
     * @param string $binary
     *
     * @return string
     */
    public function quoteBinary(string $binary): string
    {
        if ($this->connection === null) {
            throw new DatabaseException(self::DISCONNECT_ERROR);
        }
        return \pg_escape_bytea($this->connection, $binary);
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    private function castTo($value): string
    {
        $array = function ($value) use (&$array) {
            $value = \array_map(function ($value) use (&$array) {
                switch (\gettype($value)) {
                    case 'NULL':
                        return 'NULL';
                    case 'boolean':
                        return $value ? 't' : 'f';
                    case 'array':
                        return $array($value);
                    case 'integer':
                    case 'double':
                        return (string) $value;
                    case 'string':
                        $pairs = ['\\' => '\\\\', '"' => '\\"'];
                        return '"' . \strtr($value, $pairs) . '"';
                }
            }, $value);
            return '{' . \implode(',', $value) . '}';
        };
        switch (\gettype($value)) {
            case 'NULL':
                return 'NULL';
            case 'boolean':
                return $value ? 'TRUE' : 'FALSE';
            case 'array':
                return $this->quoteValue($array($value));
            case 'integer':
            case 'double':
                return (string) $value;
            case 'string':
                return $this->quoteValue($value);
        }
    }

    /**
     * @param resource $result
     *
     * @return array
     */
    private function getResultTypes($result): array
    {
        $len = \pg_num_fields($result);
        $types = [];
        for ($i = 0; $i < $len; ++$i) {
            $types[] = \pg_field_type_oid($result, $i);
        }
        return $types;
    }

    /**
     * @param string $value
     * @param int    $type
     *
     * @return mixed
     */
    private function castFrom(string $value, int $type)
    {
        switch ($type) {
            case 16: // bool
                return $value === 't';
            case 17: // bytea
                return \hex2bin(\substr($value, 2));

            case 20: // int8
            case 21: // int2
            case 23: // int4
            case 26: // oid
            case 27: // tid
            case 28: // xid
                return (int) $value;

            case 700: // real
            case 701: // double-precision
                return (float) $value;

            case 1000: // boolean[]
                return $this->parseArray($value, function ($value) {
                    return $value === 't';
                })[0];

            case 1005: // int2[]
            case 1007: // int4[]
            case 1010: // tid[]
            case 1011: // xid[]
            case 1016: // int8[]
            case 1028: // oid[]
                return $this->parseArray($value, function ($value) {
                    return (int) $value;
                })[0];

            case 1021: // real[]
            case 1022: // double-precision[]
                return $this->parseArray($value, function ($value) {
                    return (float) $value;
                })[0];

            case 1009: // text[]
                return $this->parseArray($value, null)[0];

            default:
                return $value;
        }
    }

    /**
     * @param string    $value
     * @param ?callable $cast  (optional)
     *
     * @return array
     */
    private function parseArray(string $value, ?callable $cast = null): array
    {
        $parseString = function ($value) {
            $quoted = $value[0] === '"';
            $end = $quoted ? ['"'] : [',', '}'];
            $result = [];
            $len = strlen($value);
            $pos = $quoted ? 1 : 0;
            for (; $pos < $len; $pos++) {
                if ($value[$pos] === '\\' && \in_array($value[$pos + 1], ['\\', '"'])) {
                    $pos++;
                } elseif (\in_array($value[$pos], $end, true)) {
                    break;
                }
                $result[] = $value[$pos];
            }
            $result = \implode('', $result);
            if (!$quoted && $result === 'NULL') {
                $result = null;
            }
            return [$result, $pos - ($quoted ? 0 : 1)];
        };
        $result = [];
        $len = \strlen($value);
        for ($i = 1; $i < $len; $i++) {
            switch ($value[$i]) {
                case '{':
                    [$array, $pos] = $this->parseArray(\substr($value, $i), $cast);
                    $result[] = $array;
                    $i += $pos;
                    break;
                case '}':
                    break 2;
                case ',':
                    break;
                default:
                    [$string, $pos] = $parseString(\substr($value, $i));
                    $result[] = $cast !== null ? $cast($string) : $string;
                    $i += $pos;
            }
        }
        return [$result, $i];
    }
}
