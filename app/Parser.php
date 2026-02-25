<?php

namespace App;

final class Parser
{
    private const WORKERS = 3;
    private const DATE_BITS = 12;
    private const DATE_MASK = 0xFFF;
    private const READ_BLOCK_SIZE = 1 << 20; // 1MB
    private const PATH_PREFIX_OFFSET = 25;   // "https://stitcher.io/blog/"
    private const PATH_FIXED_SUFFIX = 51;    // "," + ISO8601 tail
    private const DATE_TAIL_OFFSET = 25;     // chars between date start and line end

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \set_time_limit(0);
        \ini_set('memory_limit', '1536M');

        if (!\is_file($inputPath)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        if (!\function_exists('pcntl_fork')) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $fileSize = \filesize($inputPath);
        if (!\is_int($fileSize) || $fileSize <= 0) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $tmpDir = \dirname($outputPath) . '/.parser_tmp_' . \getmypid() . '_' . \substr(\uniqid('', true), -8);
        if (!\mkdir($tmpDir, 0777, true) && !\is_dir($tmpDir)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $fragFiles = [];
        $failed = false;

        try {
            $boundaries = $this->computeChunkBoundaries($inputPath, $fileSize, self::WORKERS);
            if ($boundaries === null) {
                $this->writeEmptyOutput($outputPath);

                return;
            }

            $pids = [];

            for ($worker = 0; $worker < self::WORKERS; $worker++) {
                $fragFiles[$worker] = $tmpDir . '/frag' . $worker;
                $chunkStart = $boundaries[$worker];
                $chunkEnd = $boundaries[$worker + 1];
                $isLastChunk = $worker === self::WORKERS - 1;

                $pid = \pcntl_fork();
                if ($pid === -1) {
                    $failed = true;
                    break;
                }

                if ($pid === 0) {
                    $ok = $this->parseChunk($inputPath, $chunkStart, $chunkEnd, $isLastChunk, $fragFiles[$worker]);
                    \exit($ok ? 0 : 1);
                }

                $pids[] = $pid;
            }

            foreach ($pids as $pid) {
                $status = 0;
                \pcntl_waitpid($pid, $status);

                if (!\pcntl_wifexited($status) || \pcntl_wexitstatus($status) !== 0) {
                    $failed = true;
                }
            }

            if ($failed) {
                $this->writeEmptyOutput($outputPath);

                return;
            }

            if (!$this->mergeFragments($fragFiles, $outputPath)) {
                $this->writeEmptyOutput($outputPath);
            }
        } finally {
            $this->cleanupTmpDir($tmpDir);
        }
    }

    private function computeChunkBoundaries(string $inputPath, int $fileSize, int $workers): ?array
    {
        $input = \fopen($inputPath, 'rb');
        if ($input === false) {
            return null;
        }

        $boundaries = [0];

        for ($worker = 1; $worker < $workers; $worker++) {
            $target = \intdiv($fileSize * $worker, $workers);
            if ($target <= 0 || $target >= $fileSize) {
                \fclose($input);

                return null;
            }

            if (\fseek($input, $target - 1) !== 0) {
                \fclose($input);

                return null;
            }

            \fgets($input);
            $boundary = \ftell($input);
            if (!\is_int($boundary) || $boundary <= $boundaries[$worker - 1] || $boundary > $fileSize) {
                \fclose($input);

                return null;
            }

            $boundaries[] = $boundary;
        }

        \fclose($input);
        $boundaries[] = $fileSize;

        return $boundaries;
    }

    private function parseChunk(
        string $inputPath,
        int $chunkStart,
        int $chunkEnd,
        bool $isLastChunk,
        string $fragmentPath
    ): bool
    {
        $input = \fopen($inputPath, 'rb');
        if ($input === false) {
            return false;
        }

        if ($chunkEnd < $chunkStart || \fseek($input, $chunkStart) !== 0) {
            \fclose($input);

            return false;
        }

        $remaining = $chunkEnd - $chunkStart;

        $pathToId = [];
        $paths = [];
        $dateToId = [];
        $dates = [];
        $firstOffsets = [];
        $counts = [];

        $carry = '';
        $carryOffset = $chunkStart;

        while ($remaining > 0) {
            $readSize = $remaining > self::READ_BLOCK_SIZE ? self::READ_BLOCK_SIZE : $remaining;
            $chunk = \fread($input, $readSize);
            if (!\is_string($chunk)) {
                \fclose($input);

                return false;
            }

            $chunkLength = \strlen($chunk);
            if ($chunkLength === 0) {
                \fclose($input);

                return false;
            }

            $remaining -= $chunkLength;

            $buffer = $carry . $chunk;
            $bufferOffset = $carryOffset;

            $lineStart = 0;
            while (true) {
                $newline = \strpos($buffer, "\n", $lineStart);
                if ($newline === false) {
                    break;
                }

                $lineLength = $newline - $lineStart;
                $path = \substr(
                    $buffer,
                    $lineStart + self::PATH_PREFIX_OFFSET,
                    $lineLength - self::PATH_FIXED_SUFFIX
                );
                $date = \substr($buffer, $newline - self::DATE_TAIL_OFFSET, 10);

                if (!isset($pathToId[$path])) {
                    $pathId = \count($paths);
                    $pathToId[$path] = $pathId;
                    $paths[$pathId] = $path;
                    $firstOffsets[$pathId] = $bufferOffset + $lineStart;
                } else {
                    $pathId = $pathToId[$path];
                }

                if (!isset($dateToId[$date])) {
                    $dateId = \count($dates);
                    if ($dateId > self::DATE_MASK) {
                        \fclose($input);

                        return false;
                    }

                    $dateToId[$date] = $dateId;
                    $dates[$dateId] = $date;
                } else {
                    $dateId = $dateToId[$date];
                }

                $key = ($pathId << self::DATE_BITS) | $dateId;
                if (isset($counts[$key])) {
                    ++$counts[$key];
                } else {
                    $counts[$key] = 1;
                }

                $lineStart = $newline + 1;
            }

            $carry = \substr($buffer, $lineStart);
            $carryOffset = $bufferOffset + $lineStart;
        }

        if ($carry !== '') {
            if (!$isLastChunk) {
                \fclose($input);

                return false;
            }

            $lineLength = \strlen($carry);
            $path = \substr($carry, self::PATH_PREFIX_OFFSET, $lineLength - self::PATH_FIXED_SUFFIX);
            $date = \substr($carry, $lineLength - self::DATE_TAIL_OFFSET, 10);

            if (!isset($pathToId[$path])) {
                $pathId = \count($paths);
                $pathToId[$path] = $pathId;
                $paths[$pathId] = $path;
                $firstOffsets[$pathId] = $carryOffset;
            } else {
                $pathId = $pathToId[$path];
            }

            if (!isset($dateToId[$date])) {
                $dateId = \count($dates);
                if ($dateId > self::DATE_MASK) {
                    \fclose($input);

                    return false;
                }

                $dateToId[$date] = $dateId;
                $dates[$dateId] = $date;
            } else {
                $dateId = $dateToId[$date];
            }

            $key = ($pathId << self::DATE_BITS) | $dateId;
            if (isset($counts[$key])) {
                ++$counts[$key];
            } else {
                $counts[$key] = 1;
            }
        }

        \fclose($input);

        $fragment = \fopen($fragmentPath, 'wb');
        if ($fragment === false) {
            return false;
        }

        $payload = \serialize([$paths, $dates, $counts, $firstOffsets]);
        $written = \fwrite($fragment, $payload);
        \fclose($fragment);

        return \is_int($written) && $written === \strlen($payload);
    }

    private function mergeFragments(array $fragmentPaths, string $outputPath): bool
    {
        $globalPathToId = [];
        $globalSlugs = [];
        $globalPathFirst = [];

        $globalDateToId = [];
        $globalDates = [];

        $globalCounts = [];

        foreach ($fragmentPaths as $fragmentPath) {
            $payload = $this->loadFragment($fragmentPath);
            if ($payload === null) {
                return false;
            }

            [$workerPaths, $workerDates, $workerCounts, $workerFirstOffsets] = $payload;

            $pathIdRemap = [];
            foreach ($workerPaths as $workerPathId => $path) {
                if (isset($globalPathToId[$path])) {
                    $globalPathId = $globalPathToId[$path];
                } else {
                    $globalPathId = \count($globalSlugs);
                    $globalPathToId[$path] = $globalPathId;
                    $globalSlugs[$globalPathId] = $path;
                }

                $pathIdRemap[$workerPathId] = $globalPathId;

                $offset = $workerFirstOffsets[$workerPathId] ?? null;
                if (\is_int($offset) && (!isset($globalPathFirst[$globalPathId]) || $offset < $globalPathFirst[$globalPathId])) {
                    $globalPathFirst[$globalPathId] = $offset;
                }
            }

            $dateIdRemap = [];
            foreach ($workerDates as $workerDateId => $date) {
                if (isset($globalDateToId[$date])) {
                    $globalDateId = $globalDateToId[$date];
                } else {
                    $globalDateId = \count($globalDates);
                    if ($globalDateId > self::DATE_MASK) {
                        return false;
                    }

                    $globalDateToId[$date] = $globalDateId;
                    $globalDates[$globalDateId] = $date;
                }

                $dateIdRemap[$workerDateId] = $globalDateId;
            }

            foreach ($workerCounts as $workerKey => $count) {
                if (!\is_int($count) || $count < 1) {
                    continue;
                }

                $workerPathId = $workerKey >> self::DATE_BITS;
                $workerDateId = $workerKey & self::DATE_MASK;

                if (!isset($pathIdRemap[$workerPathId], $dateIdRemap[$workerDateId])) {
                    continue;
                }

                $globalPathId = $pathIdRemap[$workerPathId];
                $globalDateId = $dateIdRemap[$workerDateId];
                $globalKey = ($globalPathId << self::DATE_BITS) | $globalDateId;

                if (isset($globalCounts[$globalKey])) {
                    $globalCounts[$globalKey] += $count;
                } else {
                    $globalCounts[$globalKey] = $count;
                }
            }
        }

        \asort($globalPathFirst, \SORT_NUMERIC);

        $perPathDates = [];
        foreach ($globalCounts as $globalKey => $count) {
            $pathId = $globalKey >> self::DATE_BITS;
            $dateId = $globalKey & self::DATE_MASK;

            if (!isset($globalDates[$dateId])) {
                continue;
            }

            $perPathDates[$pathId][$globalDates[$dateId]] = $count;
        }

        $output = \fopen($outputPath, 'wb');
        if ($output === false) {
            return false;
        }

        \stream_set_write_buffer($output, self::READ_BLOCK_SIZE);
        \fwrite($output, '{');

        $firstPath = true;
        foreach ($globalPathFirst as $pathId => $_offset) {
            $slug = $globalSlugs[$pathId] ?? null;
            if (!\is_string($slug)) {
                continue;
            }

            $pathKey = \json_encode('/blog/' . $slug);
            if (!\is_string($pathKey)) {
                \fclose($output);

                return false;
            }

            $dates = $perPathDates[$pathId] ?? [];
            if ($dates !== []) {
                \ksort($dates, \SORT_STRING);
            }

            if ($firstPath) {
                $firstPath = false;
            } else {
                \fwrite($output, ',');
            }

            \fwrite($output, "\n    {$pathKey}: {");

            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!\is_int($count) || $count < 1) {
                    continue;
                }

                $dateKey = \json_encode($date);
                if (!\is_string($dateKey)) {
                    \fclose($output);

                    return false;
                }

                if ($firstDate) {
                    $firstDate = false;
                } else {
                    \fwrite($output, ',');
                }

                \fwrite($output, "\n        {$dateKey}: {$count}");
            }

            \fwrite($output, "\n    }");
        }

        if ($firstPath) {
            \fwrite($output, '}');
        } else {
            \fwrite($output, "\n}");
        }

        \fclose($output);

        return true;
    }

    private function loadFragment(string $fragmentPath): ?array
    {
        if (!\is_file($fragmentPath)) {
            return null;
        }

        $raw = \file_get_contents($fragmentPath);
        if (!\is_string($raw) || $raw === '') {
            return null;
        }

        $decoded = \unserialize($raw, ['allowed_classes' => false]);
        if (!\is_array($decoded) || \count($decoded) !== 4) {
            return null;
        }

        return $decoded;
    }

    private function cleanupTmpDir(string $tmpDir): void
    {
        if (!\is_dir($tmpDir)) {
            return;
        }

        $entries = \scandir($tmpDir);
        if ($entries === false) {
            return;
        }

        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $path = $tmpDir . '/' . $entry;
            if (\is_dir($path)) {
                $this->cleanupTmpDir($path);
                if (\is_dir($path)) {
                    \rmdir($path);
                }

                continue;
            }

            if (\is_file($path)) {
                \unlink($path);
            }
        }

        if (\is_dir($tmpDir)) {
            \rmdir($tmpDir);
        }
    }

    private function writeEmptyOutput(string $outputPath): void
    {
        \file_put_contents($outputPath, '{}');
    }
}
