<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka;

use Kafka\Exception\SocketEOF;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Socket
{
    // {{{ consts

    const READ_MAX_LEN = 5242880; // read socket max length 5MB

    // }}}
    // {{{ members

    /**
     * Send timeout in seconds.
     *
     * @var float
     * @access private
     */
    private $sendTimeoutSec = 0;

    /**
     * Send timeout in microseconds.
     *
     * @var float
     * @access private
     */
    private $sendTimeoutUsec = 100000;

    /**
     * Recv timeout in seconds
     *
     * @var float
     * @access private
     */
    private $recvTimeoutSec = 0;

    /**
     * Recv timeout in microseconds
     *
     * @var float
     * @access private
     */
    private $recvTimeoutUsec = 750000;

    /**
     * Stream resource
     *
     * @var mixed
     * @access private
     */
    private $stream = null;

    /**
     * Socket host
     *
     * @var mixed
     * @access private
     */
    private $host = null;

    /**
     * Socket port
     *
     * @var int
     * @access private
     */
    private $port = -1;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct($host, $port, $recvTimeoutSec = 0, $recvTimeoutUsec = 750000, $sendTimeoutSec = 0, $sendTimeoutUsec = 100000)
    {
        $this->host = $host;
        $this->port = $port;
        $this->recvTimeoutSec  = $recvTimeoutSec;
        $this->recvTimeoutUsec = $recvTimeoutUsec;
        $this->sendTimeoutSec  = $sendTimeoutSec;
        $this->sendTimeoutUsec = $sendTimeoutUsec;
    }

    // }}}
    // {{{ public static function createFromStream()

    /**
     * Optional method to set the internal stream handle
     *
     * @static
     * @access public
     * @return Socket
     */
    public static function createFromStream($stream)
    {
        $socket = new self('localhost', 0);
        $socket->setStream($stream);
        return $socket;
    }

    // }}}
    // {{{ public function setStream()

    /**
     * Optional method to set the internal stream handle
     *
     * @param mixed $stream
     * @access public
     * @return $this
     * @throws \InvalidArgumentException
     */
    public function setStream($stream)
    {
        if (!is_resource($stream)) {
            throw new \InvalidArgumentException(
                sprintf(
                    'Stream should be a resource, %s given',
                    is_object($stream) ? get_class($stream) : gettype($stream)
                )
            );
        }
        $this->stream = $stream;
        return $this;
    }

    // }}}
    // {{{ public function connect()

    /**
     * Connects the socket
     *
     * @access public
     * @return $this
     */
    public function connect()
    {
        if (!is_resource($this->stream)) {
            if (empty($this->host)) {
                throw new \Kafka\Exception('Cannot open null host.');
            }
            if ($this->port <= 0) {
                throw new \Kafka\Exception('Cannot open without port.');
            }
    
    
            $this->stream = fsockopen(
                $this->host,
                $this->port,
                $errno,
                $errstr,
                $this->sendTimeoutSec + ($this->sendTimeoutUsec / 1000000)
            );
    
            if ($this->stream == false) {
                throw new \RuntimeException(
                    sprintf(
                        'Could not connect to %s:%d -> %s (%d)',
                        $this->host,
                        $this->port,
                        $errstr,
                        $errno
                    )
                );
            }
    
            stream_set_blocking($this->stream, 0);
        }
        return $this;
    }

    // }}}
    // {{{ public function close()

    /**
     * close the socket
     *
     * @access public
     * @return $this
     */
    public function close()
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
        return $this;
    }

    // }}}
    // {{{ public function read()

    /**
     * Read from the socket at most $len bytes.
     *
     * This method will not wait for all the requested data, it will return as
     * soon as any data is received.
     *
     * @param integer $len               Maximum number of bytes to read.
     * @param boolean $verifyExactLength Throw an exception if the number of read bytes is less than $len
     *
     * @return string Binary data
     * @throws Kafka_Exception_Socket
     */
    public function read($len, $verifyExactLength = false)
    {
        if ($len > self::READ_MAX_LEN) {
            throw new \InvalidArgumentException(
                sprintf(
                    'Unable to read %d bytes from stream, max length is %d',
                    $len,
                    self::READ_MAX_LEN
                )
            );
        }

        //use separate variables (to keep refcount at 1)
        $write = null;
        //use temp stream for errors that might occur?
        $ex = null;//array(fopen('php://temp', 'w+'));
        //if ($ex[0] === false) {
        //    $ex = null;
        //}
        $read = array($this->stream);
        $readable = stream_select(
            $read,
            $write,
            $ex,
            $this->recvTimeoutSec,
            $this->recvTimeoutUsec
        );
        $remainingBytes = $len;
        $data = '';
        while($readable !== false && $remainingBytes) {
            //if no stream was altered, then do nothing
            if ($readable) {
                $chunk = fread($this->stream, $remainingBytes);
                if ($chunk === false) {
                    throw new \RuntimeException(
                        sprintf(
                            'Error reading %d bytes from stream',
                            $remainingBytes
                        )
                    );
                }
                $bytesRead = strlen($chunk);
                if ($bytesRead) {
                    $data .= $chunk;
                    $remainingBytes -= $bytesRead;
                } elseif (feof($this->stream)) {
                    //reached end of stream, but we could've read something
                    //EOF or max bytes, whichever comes first, this is NOT an exception-case
                    break;
                } else {
                    //streams HAVE changed, no data gotten by fread, and stream is not EOF
                    //check timeout
                    $res = stream_get_meta_data($this->stream);
                    if (!empty($res['timed_out'])) {
                        throw new \RuntimeException(
                            sprintf(
                                'stream timed out reading %d bytes',
                                $len
                            )
                        );
                    }
                    //nothing was read, but some stream WAS altered, check $ex stream?
                    throw new \RuntimeException(
                        'Something was written to write or except streams?'
                    );
                }
            }
            //check stream again, keep doing so until we've read all bytes
            //or feof $this->stream is true
            $readable = stream_select(
                $read,
                $write,
                $ex,
                $this->recvTimeoutSec,
                $this->recvTimeoutUsec
            );
        }
        if ($remainingBytes && $verifyExactLength) {
            //we needed exactly $len bytes, but we fell $remainingBytes short
            throw new SocketEOF(
                sprintf(
                    'Needed to read %d bytes, instead read %d bytes (%d short)',
                    $len,
                    $len - $remainingBytes,
                    $remainingBytes
                )
            );
        }
        return $data;
    }

    // }}}
    // {{{ public function write()

    /**
     * Write to the socket.
     *
     * @param string $buf The data to write
     *
     * @return integer
     * @throws Kafka_Exception_Socket
     */
    public function write($buf)
    {
        $null = null;
        $write = array($this->stream);

        // fwrite to a socket may be partial, so loop until we
        // are done with the entire buffer
        $written = 0;
        $buflen = strlen($buf);
        while ( $written < $buflen ) {
            // wait for stream to become available for writing
            $writable = stream_select($null, $write, $null, $this->sendTimeoutSec, $this->sendTimeoutUsec);
            if ($writable > 0) {
                // write remaining buffer bytes to stream
                $wrote = fwrite($this->stream, substr($buf, $written));
                if ($wrote === -1 || $wrote === false) {
                    throw new \Kafka\Exception\Socket('Could not write ' . strlen($buf) . ' bytes to stream, completed writing only ' . $written . ' bytes');
                }
                $written += $wrote;
                continue;
            }
            if (false !== $writable) {
                $res = stream_get_meta_data($this->stream);
                if (!empty($res['timed_out'])) {
                    throw new \Kafka\Exception\SocketTimeout('Timed out writing ' . strlen($buf) . ' bytes to stream after writing ' . $written . ' bytes');
                }
            }
            throw new \Kafka\Exception\Socket('Could not write ' . strlen($buf) . ' bytes to stream');
        }
        return $written;
    }

    // }}}
    // {{{ public function rewind()

    /**
     * Rewind the stream
     *
     * @return $this
     */
    public function rewind()
    {
        if (is_resource($this->stream)) {
            rewind($this->stream);
        }
        return $this;
    }
    // }}}
    // }}}
    /**
     * Simple destructor, ensuring the socket is closed when the instance is GC'ed
     */
    public function __destruct()
    {
        $this->close();
    }
}
