<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        // One knob. Change ONLY this.
        $workers = 1; // macOS: 1.  DO droplet (2 vCPU): 2.

        // Hard-disable warnings -> exceptions in frameworks
        \set_error_handler(static function () { return true; });
        \gc_disable();
        @\set_time_limit(0);
        @\ini_set('memory_limit', '1536M');

        // ====== TUNABLES (leave these alone) ======
        $bucketCount = 8192;   // power of 2
        $groupSize   = 128;    // power of 2; keep <=128 to avoid macOS FD limits
        $groupShift  = 7;      // log2(groupSize)
        $mask        = $bucketCount - 1;
        $groupCount  = $bucketCount >> $groupShift;

        // Buffers
        $inReadBuf      = 1 << 23; // 8MB
        $spillWriteBuf  = 1 << 20; // 1MB
        $spillReadBuf   = 1 << 23; // 8MB
        $bucketWriteBuf = 1 << 18; // 256KB * 128 = 32MB worst-case buffers
        $bucketReadBuf  = 1 << 23; // 8MB
        $fragWriteBuf   = 1 << 24; // 16MB
        $flushThreshold = 1 << 20; // 1MB

        $baseDir = \dirname($outputPath);
        $tmpDir  = $baseDir . '/.parser_tmp_' . \getmypid() . '_' . \substr(\uniqid('', true), -8);
        if (!\is_dir($tmpDir) && !@\mkdir($tmpDir, 0777, true) && !\is_dir($tmpDir)) {
            \file_put_contents($outputPath, '{}');
            return;
        }

        // ============================================================
        // PASS 1: Single scan -> group spill files
        // Spill record: [1 byte off][path],[YYYY-MM-DD]\n
        // ============================================================
        $spillFiles = [];
        $spillH = [];
        for ($g = 0; $g < $groupCount; $g++) {
            $spillFiles[$g] = $tmpDir . '/s' . $g;
            $h = \fopen($spillFiles[$g], 'wb');
            if ($h === false) {
                \file_put_contents($outputPath, '{}');
                return;
            }
            \stream_set_write_buffer($h, $spillWriteBuf);
            $spillH[$g] = $h;
        }

        $in = \fopen($inputPath, 'rb');
        if ($in === false) {
            foreach ($spillH as $h) { \fclose($h); }
            \file_put_contents($outputPath, '{}');
            return;
        }
        \stream_set_read_buffer($in, $inReadBuf);
        $pathOrder = [];
        $pathSeen = [];

        while (($line = \fgets($in)) !== false) {
            $comma = \strpos($line, ',');
            if ($comma === false) continue;

            $date = \substr($line, $comma + 1, 10);

            // Fast url->path (controlled input)
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

            if (!isset($pathSeen[$path])) {
                $pathSeen[$path] = true;
                $pathOrder[] = $path;
            }

            $bucket = \crc32($path) & $mask;
            $g      = $bucket >> $groupShift;
            $off    = $bucket & ($groupSize - 1);

            // Keep the bucket marker out of ASCII control chars (\n, \r, etc.).
            \fwrite($spillH[$g], \chr($off | 0x80) . $path . ',' . $date . "\n");
        }

        \fclose($in);
        foreach ($spillH as $h) { \fclose($h); }

        // ============================================================
        // PASS 2+3: reduce (serial or parallel depending on $workers)
        // Each worker writes a fragment containing top-level entries (no braces)
        // ============================================================
        if ($workers <= 1 || !\function_exists('pcntl_fork')) {
            // Serial: write a single fragment then merge it.
            $frag0 = $tmpDir . '/frag0';
            $this->reduceWorker(
                0, 1,
                $spillFiles, $tmpDir,
                $groupCount, $groupSize,
                $spillReadBuf,
                $bucketWriteBuf, $bucketReadBuf,
                $frag0,
                $fragWriteBuf, $flushThreshold
            );

            $this->mergeFragments([$frag0], $outputPath, $fragWriteBuf, $pathOrder);
            return;
        }

        // Parallel
        $workers = (int)$workers;
        if ($workers < 2) $workers = 2;

        $fragFiles = [];
        for ($w = 0; $w < $workers; $w++) {
            $fragFiles[$w] = $tmpDir . '/frag' . $w;
        }

        $pids = [];
        for ($w = 0; $w < $workers; $w++) {
            $pid = \pcntl_fork();
            if ($pid === -1) {
                // Fork failed -> fall back to serial
                $frag0 = $tmpDir . '/frag0';
                $this->reduceWorker(
                    0, 1,
                    $spillFiles, $tmpDir,
                    $groupCount, $groupSize,
                    $spillReadBuf,
                    $bucketWriteBuf, $bucketReadBuf,
                    $frag0,
                    $fragWriteBuf, $flushThreshold
                );
                $this->mergeFragments([$frag0], $outputPath, $fragWriteBuf, $pathOrder);
                return;
            }

            if ($pid === 0) {
                $this->reduceWorker(
                    $w, $workers,
                    $spillFiles, $tmpDir,
                    $groupCount, $groupSize,
                    $spillReadBuf,
                    $bucketWriteBuf, $bucketReadBuf,
                    $fragFiles[$w],
                    $fragWriteBuf, $flushThreshold
                );
                \exit(0);
            }

            $pids[] = $pid;
        }

        foreach ($pids as $pid) {
            \pcntl_waitpid($pid, $status);
        }

        $this->mergeFragments($fragFiles, $outputPath, $fragWriteBuf, $pathOrder);
    }

    private function reduceWorker(
        int $workerId,
        int $workers,
        array $spillFiles,
        string $tmpDir,
        int $groupCount,
        int $groupSize,
        int $spillReadBuf,
        int $bucketWriteBuf,
        int $bucketReadBuf,
        string $fragFile,
        int $fragWriteBuf,
        int $flushThreshold
    ): void {
        $fh = \fopen($fragFile, 'wb');
        if ($fh === false) return;
        \stream_set_write_buffer($fh, $fragWriteBuf);

        $firstTop = true;
        $buf = '';

        for ($g = $workerId; $g < $groupCount; $g += $workers) {
            $spillFile = $spillFiles[$g] ?? null;
            if ($spillFile === null || !\is_file($spillFile) || \filesize($spillFile) === 0) {
                continue;
            }

            $gDir = $tmpDir . '/g' . $g;
            if (!\is_dir($gDir) && !@\mkdir($gDir, 0777, true) && !\is_dir($gDir)) {
                continue;
            }

            // Route spill -> groupSize buckets (open <=128 files)
            $bucketH = [];
            for ($off = 0; $off < $groupSize; $off++) {
                $bh = \fopen($gDir . '/' . $off, 'wb');
                if ($bh === false) {
                    foreach ($bucketH as $x) { \fclose($x); }
                    \fclose($fh);
                    return;
                }
                \stream_set_write_buffer($bh, $bucketWriteBuf);
                $bucketH[$off] = $bh;
            }

            $sh = \fopen($spillFile, 'rb');
            if ($sh === false) {
                foreach ($bucketH as $x) { \fclose($x); }
                continue;
            }
            \stream_set_read_buffer($sh, $spillReadBuf);

            while (($rec = \fgets($sh)) !== false) {
                $off = \ord($rec[0]) & 0x7F;
                \fwrite($bucketH[$off], $rec);
            }

            \fclose($sh);
            foreach ($bucketH as $x) { \fclose($x); }

            // Reduce each bucket -> fragment
            for ($off = 0; $off < $groupSize; $off++) {
                $bf = $gDir . '/' . $off;
                if (!\is_file($bf) || \filesize($bf) === 0) {
                    continue;
                }

                $bh = \fopen($bf, 'rb');
                if ($bh === false) continue;
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
        }

        if ($buf !== '') \fwrite($fh, $buf);
        \fclose($fh);
    }

    private function mergeFragments(array $fragFiles, string $outputPath, int $outBuf, array $pathOrder): void
    {
        $outH = \fopen($outputPath, 'wb');
        if ($outH === false) {
            \file_put_contents($outputPath, '{}');
            return;
        }
        \stream_set_write_buffer($outH, $outBuf);

        \fwrite($outH, '{');

        $wroteAny = false;
        foreach ($fragFiles as $frag) {
            if (!\is_file($frag) || \filesize($frag) === 0) continue;

            if ($wroteAny) \fwrite($outH, ',');
            $wroteAny = true;

            $fh = \fopen($frag, 'rb');
            if ($fh !== false) {
                \stream_copy_to_stream($fh, $outH);
                \fclose($fh);
            }
        }

        \fwrite($outH, '}');
        \fclose($outH);

        $json = \file_get_contents($outputPath);
        if ($json === false || $json === '') {
            \file_put_contents($outputPath, '{}');
            return;
        }

        $decoded = \json_decode($json, true);
        if (!\is_array($decoded)) {
            \file_put_contents($outputPath, '{}');
            return;
        }

        $ordered = [];
        foreach ($pathOrder as $path) {
            $dates = $decoded[$path] ?? null;
            if (!\is_array($dates)) {
                continue;
            }

            \ksort($dates);
            $ordered[$path] = $dates;
            unset($decoded[$path]);
        }

        // Keep any unexpected keys while still writing deterministic dates.
        foreach ($decoded as $path => $dates) {
            if (!\is_array($dates)) {
                continue;
            }

            \ksort($dates);
            $ordered[$path] = $dates;
        }

        $pretty = \json_encode($ordered, \JSON_PRETTY_PRINT);
        if (!\is_string($pretty)) {
            \file_put_contents($outputPath, '{}');
            return;
        }

        \file_put_contents($outputPath, $pretty);
    }
}
