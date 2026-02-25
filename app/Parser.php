<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        @\set_time_limit(0);
        @\ini_set('memory_limit', '1536M');

        // ---- TUNABLES for DO 2vCPU / 1.5GB ----
        $bucketCount = 8192;     // power of 2
        $groupSize   = 256;      // power of 2, <=256 so bucketOffset fits in 1 byte
        $groupShift  = 8;        // log2(groupSize)
        $mask        = $bucketCount - 1;
        $groupCount  = $bucketCount >> $groupShift;

        // Buffers
        $inReadBuf      = 1 << 23; // 8MB
        $spillWriteBuf  = 1 << 18; // 256KB
        $spillReadBuf   = 1 << 23; // 8MB
        $bucketWriteBuf = 1 << 18; // 256KB per open bucket stream (x256 => 64MB buffers)
        $bucketReadBuf  = 1 << 23; // 8MB
        $fragWriteBuf   = 1 << 24; // 16MB
        $flushThreshold = 1 << 20; // 1MB

        $workers = 2; // you said pcntl is available; droplet has 2 vCPUs

        $baseDir = \dirname($outputPath);
        $tmpDir  = $baseDir . '/.parser_tmp_' . \getmypid() . '_' . \substr(\uniqid('', true), -8);
        if (!@\mkdir($tmpDir, 0777, true) && !\is_dir($tmpDir)) {
            \file_put_contents($outputPath, '{}');
            return;
        }

        // ============================================================
        // PASS 1: Single scan -> group spill files
        // Spill record: [1 byte bucketOffset][path],[YYYY-MM-DD]\n
        // ============================================================
        $spillH = [];
        for ($g = 0; $g < $groupCount; $g++) {
            $h = \fopen($tmpDir . '/s' . $g, 'wb');
            if ($h === false) {
                $this->cleanupTmp($tmpDir);
                \file_put_contents($outputPath, '{}');
                return;
            }
            \stream_set_write_buffer($h, $spillWriteBuf);
            $spillH[$g] = $h;
        }

        $in = \fopen($inputPath, 'rb');
        if ($in === false) {
            foreach ($spillH as $h) { \fclose($h); }
            $this->cleanupTmp($tmpDir);
            \file_put_contents($outputPath, '{}');
            return;
        }
        \stream_set_read_buffer($in, $inReadBuf);

        while (($line = \fgets($in)) !== false) {
            $comma = \strpos($line, ',');
            if ($comma === false) continue;

            $date = \substr($line, $comma + 1, 10);

            if ($line[0] === '/') {
                $path = \substr($line, 0, $comma);
            } else {
                $slash = \strpos($line, '/', 8);
                if ($slash === false || $slash >= $comma) {
                    $path = \substr($line, 0, $comma);
                } else {
                    $path = \substr($line, $slash, $comma - $slash);
                }
            }

            $bucket = \crc32($path) & $mask;
            $g      = $bucket >> $groupShift;
            $off    = $bucket & ($groupSize - 1);

            \fwrite($spillH[$g], \chr($off) . $path . ',' . $date . "\n");
        }

        \fclose($in);
        foreach ($spillH as $h) { \fclose($h); }

        // ============================================================
        // PASS 2+3 in parallel: for each group -> route into buckets -> reduce -> fragment
        // ============================================================
        $fragFiles = [
            $tmpDir . '/frag0',
            $tmpDir . '/frag1',
        ];

        $pids = [];
        for ($w = 0; $w < $workers; $w++) {
            $pid = \pcntl_fork();
            if ($pid === -1) {
                // Fork failed; fall back to single worker (run in parent)
                $workers = 1;
                break;
            }

            if ($pid === 0) {
                // CHILD
                $frag = $fragFiles[$w];
                $fh = \fopen($frag, 'wb');
                if ($fh === false) {
                    \exit(1);
                }
                \stream_set_write_buffer($fh, $fragWriteBuf);

                $firstTop = true;
                $buf = '';

                for ($g = $w; $g < $groupCount; $g += $workers) {
                    $spillFile = $tmpDir . '/s' . $g;
                    if (!\is_file($spillFile) || \filesize($spillFile) === 0) {
                        @\unlink($spillFile);
                        continue;
                    }

                    // ---- route spill -> 256 bucket files (only for this group) ----
                    $gDir = $tmpDir . '/g' . $g;
                    @\mkdir($gDir, 0777, true);

                    $bucketH = [];
                    for ($off = 0; $off < $groupSize; $off++) {
                        $bh = \fopen($gDir . '/' . $off, 'wb');
                        if ($bh === false) {
                            foreach ($bucketH as $x) { \fclose($x); }
                            \fclose($fh);
                            \exit(2);
                        }
                        \stream_set_write_buffer($bh, $bucketWriteBuf);
                        $bucketH[$off] = $bh;
                    }

                    $sh = \fopen($spillFile, 'rb');
                    if ($sh === false) {
                        foreach ($bucketH as $x) { \fclose($x); }
                        \fclose($fh);
                        \exit(3);
                    }
                    \stream_set_read_buffer($sh, $spillReadBuf);

                    while (($rec = \fgets($sh)) !== false) {
                        $off = \ord($rec[0]);
                        // Write whole line (includes off byte). Avoid substr() copy here.
                        \fwrite($bucketH[$off], $rec);
                    }

                    \fclose($sh);
                    @\unlink($spillFile);
                    foreach ($bucketH as $x) { \fclose($x); }

                    // ---- reduce each bucket immediately -> fragment ----
                    for ($off = 0; $off < $groupSize; $off++) {
                        $bf = $gDir . '/' . $off;
                        if (!\is_file($bf) || \filesize($bf) === 0) {
                            @\unlink($bf);
                            continue;
                        }

                        $bh = \fopen($bf, 'rb');
                        if ($bh === false) {
                            @\unlink($bf);
                            continue;
                        }
                        \stream_set_read_buffer($bh, $bucketReadBuf);

                        $map = [];
                        while (($line = \fgets($bh)) !== false) {
                            // [1 byte off][path],[YYYY-MM-DD]\n
                            $comma = \strpos($line, ',', 1);
                            if ($comma === false) continue;

                            $path = \substr($line, 1, $comma - 1);
                            $date = \substr($line, $comma + 1, 10);

                            if (isset($map[$path][$date])) {
                                ++$map[$path][$date];
                            } else {
                                $map[$path][$date] = 1;
                            }
                        }
                        \fclose($bh);
                        @\unlink($bf);

                        foreach ($map as $path => $dates) {
                            if ($firstTop) $firstTop = false;
                            else $buf .= ',';

                            $buf .= '"' . $path . '":{';

                            $firstDate = true;
                            foreach ($dates as $date => $count) {
                                if ($firstDate) $firstDate = false;
                                else $buf .= ',';
                                $buf .= '"' . $date . '":' . $count;
                            }

                            $buf .= '}';

                            if (\strlen($buf) >= $flushThreshold) {
                                \fwrite($fh, $buf);
                                $buf = '';
                            }
                        }

                        unset($map);
                    }

                    @\rmdir($gDir);
                }

                if ($buf !== '') \fwrite($fh, $buf);
                \fclose($fh);
                \exit(0);
            }

            // PARENT
            $pids[] = $pid;
        }

        if ($workers === 1) {
            // No fork; do child work in-process and write to frag0.
            $frag = $fragFiles[0];
            $fh = \fopen($frag, 'wb');
            if ($fh === false) {
                $this->cleanupTmp($tmpDir);
                \file_put_contents($outputPath, '{}');
                return;
            }
            \stream_set_write_buffer($fh, $fragWriteBuf);

            $firstTop = true;
            $buf = '';

            for ($g = 0; $g < $groupCount; $g++) {
                $spillFile = $tmpDir . '/s' . $g;
                if (!\is_file($spillFile) || \filesize($spillFile) === 0) {
                    @\unlink($spillFile);
                    continue;
                }

                $gDir = $tmpDir . '/g' . $g;
                @\mkdir($gDir, 0777, true);

                $bucketH = [];
                for ($off = 0; $off < $groupSize; $off++) {
                    $bh = \fopen($gDir . '/' . $off, 'wb');
                    if ($bh === false) {
                        foreach ($bucketH as $x) { \fclose($x); }
                        \fclose($fh);
                        $this->cleanupTmp($tmpDir);
                        \file_put_contents($outputPath, '{}');
                        return;
                    }
                    \stream_set_write_buffer($bh, $bucketWriteBuf);
                    $bucketH[$off] = $bh;
                }

                $sh = \fopen($spillFile, 'rb');
                if ($sh === false) {
                    foreach ($bucketH as $x) { \fclose($x); }
                    \fclose($fh);
                    $this->cleanupTmp($tmpDir);
                    \file_put_contents($outputPath, '{}');
                    return;
                }
                \stream_set_read_buffer($sh, $spillReadBuf);

                while (($rec = \fgets($sh)) !== false) {
                    $off = \ord($rec[0]);
                    \fwrite($bucketH[$off], $rec);
                }

                \fclose($sh);
                @\unlink($spillFile);
                foreach ($bucketH as $x) { \fclose($x); }

                for ($off = 0; $off < $groupSize; $off++) {
                    $bf = $gDir . '/' . $off;
                    if (!\is_file($bf) || \filesize($bf) === 0) {
                        @\unlink($bf);
                        continue;
                    }

                    $bh = \fopen($bf, 'rb');
                    if ($bh === false) {
                        @\unlink($bf);
                        continue;
                    }
                    \stream_set_read_buffer($bh, $bucketReadBuf);

                    $map = [];
                    while (($line = \fgets($bh)) !== false) {
                        $comma = \strpos($line, ',', 1);
                        if ($comma === false) continue;

                        $path = \substr($line, 1, $comma - 1);
                        $date = \substr($line, $comma + 1, 10);

                        if (isset($map[$path][$date])) ++$map[$path][$date];
                        else $map[$path][$date] = 1;
                    }

                    \fclose($bh);
                    @\unlink($bf);

                    foreach ($map as $path => $dates) {
                        if ($firstTop) $firstTop = false;
                        else $buf .= ',';

                        $buf .= '"' . $path . '":{';

                        $firstDate = true;
                        foreach ($dates as $date => $count) {
                            if ($firstDate) $firstDate = false;
                            else $buf .= ',';
                            $buf .= '"' . $date . '":' . $count;
                        }

                        $buf .= '}';

                        if (\strlen($buf) >= $flushThreshold) {
                            \fwrite($fh, $buf);
                            $buf = '';
                        }
                    }

                    unset($map);
                }

                @\rmdir($gDir);
            }

            if ($buf !== '') \fwrite($fh, $buf);
            \fclose($fh);
        } else {
            foreach ($pids as $pid) {
                \pcntl_waitpid($pid, $status);
            }
        }

        // ============================================================
        // FINAL MERGE: { frag0 , frag1 }
        // ============================================================
        $outH = \fopen($outputPath, 'wb');
        if ($outH === false) {
            \file_put_contents($outputPath, '{}');
            return;
        }
        \stream_set_write_buffer($outH, $fragWriteBuf);

        \fwrite($outH, '{');

        $wroteAny = false;
        for ($i = 0; $i < 2; $i++) {
            $frag = $fragFiles[$i];
            if (!\is_file($frag) || \filesize($frag) === 0) {
                @\unlink($frag);
                continue;
            }

            if ($wroteAny) \fwrite($outH, ',');
            $wroteAny = true;

            $fh = \fopen($frag, 'rb');
            if ($fh !== false) {
                \stream_copy_to_stream($fh, $outH);
                \fclose($fh);
            }
            @\unlink($frag);
        }

        \fwrite($outH, '}');
        \fclose($outH);
    }
}