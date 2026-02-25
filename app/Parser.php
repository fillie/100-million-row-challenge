<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    private const WORKERS = 3;
    private const READ_BLOCK_SIZE = 1 << 20; // 1 MiB
    private const DATE_BITS = 12;
    private const DATE_MASK = 0xFFF;
    private const PATH_PREFIX_OFFSET = 25; // "https://stitcher.io/blog/"
    private const PATH_FIXED_SUFFIX = 51;  // ",YYYY-MM-DDTHH:MM:SS+00:00"
    private const DATE_TAIL_OFFSET = 25;   // chars from date start to line end
    private const UNKNOWN_OFFSET_SENTINEL = -1;

    public function parse(string $inputPath, string $outputPath): void
    {
        \gc_disable();
        \set_time_limit(0);
        \ini_set('memory_limit', '1536M');

        if (!\is_file($inputPath)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        if (!\function_exists('pcntl_fork') || !\function_exists('stream_socket_pair') || !\function_exists('stream_select')) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $fileSize = \filesize($inputPath);
        if (!\is_int($fileSize) || $fileSize <= 0) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        [$knownSlugToId, $knownIdToSlug] = $this->buildKnownPathSpaceFromVisitAll();
        [$knownDateToId, $knownIdToDate] = $this->buildKnownDateSpaceExactFiveYearsUtc();

        $knownPathCount = \count($knownIdToSlug);
        $knownDateCount = \count($knownIdToDate);

        if ($knownDateCount === 0 || $knownDateCount > (self::DATE_MASK + 1)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        $denseArrayLength = $knownPathCount * $knownDateCount;

        $tmpDir = \dirname($outputPath) . '/.parser_tmp_' . \getmypid() . '_' . \substr(\uniqid('', true), -8);
        if (!\mkdir($tmpDir, 0777, true) && !\is_dir($tmpDir)) {
            $this->writeEmptyOutput($outputPath);

            return;
        }

        try {
            $boundaries = $this->computeChunkBoundaries($inputPath, $fileSize, self::WORKERS);
            if ($boundaries === null) {
                $this->writeEmptyOutput($outputPath);

                return;
            }

            $parentSockets = [];
            $socketToWorker = [];
            $pids = [];
            $failed = false;

            $ipProto = \defined('STREAM_IPPROTO_IP') ? \STREAM_IPPROTO_IP : 0;

            for ($worker = 0; $worker < self::WORKERS; $worker++) {
                $pair = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, $ipProto);
                if ($pair === false) {
                    $failed = true;
                    break;
                }

                $chunkStart = $boundaries[$worker];
                $chunkEnd = $boundaries[$worker + 1];
                $isLastChunk = $worker === self::WORKERS - 1;

                $pid = \pcntl_fork();
                if ($pid === -1) {
                    \fclose($pair[0]);
                    \fclose($pair[1]);
                    $failed = true;
                    break;
                }

                if ($pid === 0) {
                    foreach ($parentSockets as $parentSocket) {
                        \fclose($parentSocket);
                    }

                    \fclose($pair[0]);

                    $ok = $this->parseChunkAndSend(
                        $pair[1],
                        $inputPath,
                        $chunkStart,
                        $chunkEnd,
                        $isLastChunk,
                        $knownSlugToId,
                        $knownDateToId,
                        $knownPathCount,
                        $knownDateCount
                    );

                    \fclose($pair[1]);
                    \exit($ok ? 0 : 1);
                }

                \fclose($pair[1]);
                \stream_set_blocking($pair[0], false);

                $parentSockets[$worker] = $pair[0];
                $socketToWorker[(int)$pair[0]] = $worker;
                $pids[$worker] = $pid;
            }

            if ($failed) {
                foreach ($parentSockets as $socket) {
                    \fclose($socket);
                }

                foreach ($pids as $pid) {
                    \pcntl_waitpid($pid, $status);
                }

                $this->writeEmptyOutput($outputPath);

                return;
            }

            $workerPayloads = $this->readAllWorkerPayloads($parentSockets, $socketToWorker);
            if ($workerPayloads === null) {
                $failed = true;
            }

            foreach ($pids as $pid) {
                \pcntl_waitpid($pid, $status);

                if (!\pcntl_wifexited($status) || \pcntl_wexitstatus($status) !== 0) {
                    $failed = true;
                }
            }

            if ($failed || $workerPayloads === null) {
                $this->writeEmptyOutput($outputPath);

                return;
            }

            $globalDenseCounts = \array_fill(0, $denseArrayLength, 0);
            $globalKnownFirstOffsets = \array_fill(0, $knownPathCount, self::UNKNOWN_OFFSET_SENTINEL);
            $globalFallbackCounts = [];
            $globalFallbackFirstOffsets = [];

            for ($worker = 0; $worker < self::WORKERS; $worker++) {
                $payload = $workerPayloads[$worker] ?? null;
                if (!\is_string($payload) || !$this->mergeWorkerPayload(
                    $payload,
                    $denseArrayLength,
                    $knownPathCount,
                    $globalDenseCounts,
                    $globalKnownFirstOffsets,
                    $globalFallbackCounts,
                    $globalFallbackFirstOffsets
                )) {
                    $this->writeEmptyOutput($outputPath);

                    return;
                }
            }

            if (!$this->writeOutput(
                $outputPath,
                $knownSlugToId,
                $knownIdToSlug,
                $knownIdToDate,
                $knownDateCount,
                $globalDenseCounts,
                $globalKnownFirstOffsets,
                $globalFallbackCounts,
                $globalFallbackFirstOffsets
            )) {
                $this->writeEmptyOutput($outputPath);
            }
        } finally {
            $this->cleanupTmpDir($tmpDir);
        }
    }

    private function buildKnownPathSpaceFromVisitAll(): array
    {
        $knownSlugToId = [];
        $knownIdToSlug = [];

        foreach (Visit::all() as $visit) {
            $slug = $this->extractSlugFromUri($visit->uri);
            if (!\is_string($slug) || $slug === '') {
                continue;
            }

            if (!isset($knownSlugToId[$slug])) {
                $pathId = \count($knownIdToSlug);
                $knownSlugToId[$slug] = $pathId;
                $knownIdToSlug[$pathId] = $slug;
            }
        }

        return [$knownSlugToId, $knownIdToSlug];
    }

    private function extractSlugFromUri(string $uri): ?string
    {
        $blogPos = \strpos($uri, '/blog/');
        if ($blogPos !== false) {
            return \substr($uri, $blogPos + 6);
        }

        if (\str_starts_with($uri, '/blog/')) {
            return \substr($uri, 6);
        }

        return null;
    }

    private function buildKnownDateSpaceExactFiveYearsUtc(): array
    {
        $knownDateToId = [];
        $knownIdToDate = [];

        $today = new \DateTimeImmutable('today', new \DateTimeZone('UTC'));
        $start = $today->sub(new \DateInterval('P5Y'));
        $step = new \DateInterval('P1D');

        for ($cursor = $start; $cursor <= $today; $cursor = $cursor->add($step)) {
            $date = $cursor->format('Y-m-d');
            $dateId = \count($knownIdToDate);
            $knownDateToId[$date] = $dateId;
            $knownIdToDate[$dateId] = $date;
        }

        return [$knownDateToId, $knownIdToDate];
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

    private function parseChunkAndSend(
        $socket,
        string $inputPath,
        int $chunkStart,
        int $chunkEnd,
        bool $isLastChunk,
        array $knownSlugToId,
        array $knownDateToId,
        int $knownPathCount,
        int $knownDateCount
    ): bool {
        $input = \fopen($inputPath, 'rb');
        if ($input === false) {
            return false;
        }

        if ($chunkEnd < $chunkStart || \fseek($input, $chunkStart) !== 0) {
            \fclose($input);

            return false;
        }

        $denseCounts = \array_fill(0, $knownPathCount * $knownDateCount, 0);
        $knownFirstOffsets = \array_fill(0, $knownPathCount, self::UNKNOWN_OFFSET_SENTINEL);
        $fallbackCounts = [];
        $fallbackFirstOffsets = [];

        $remaining = $chunkEnd - $chunkStart;
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
                $slug = \substr(
                    $buffer,
                    $lineStart + self::PATH_PREFIX_OFFSET,
                    $lineLength - self::PATH_FIXED_SUFFIX
                );
                $date = \substr($buffer, $newline - self::DATE_TAIL_OFFSET, 10);
                $absoluteOffset = $bufferOffset + $lineStart;

                $knownPathId = $knownSlugToId[$slug] ?? null;
                $knownDateId = $knownDateToId[$date] ?? null;

                if (\is_int($knownPathId) && \is_int($knownDateId)) {
                    $denseIndex = $knownPathId * $knownDateCount + $knownDateId;
                    ++$denseCounts[$denseIndex];

                    $existingOffset = $knownFirstOffsets[$knownPathId];
                    if ($existingOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $existingOffset) {
                        $knownFirstOffsets[$knownPathId] = $absoluteOffset;
                    }
                } else {
                    if (\is_int($knownPathId)) {
                        $existingOffset = $knownFirstOffsets[$knownPathId];
                        if ($existingOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $existingOffset) {
                            $knownFirstOffsets[$knownPathId] = $absoluteOffset;
                        }
                    }

                    if (!isset($fallbackCounts[$slug][$date])) {
                        $fallbackCounts[$slug][$date] = 1;
                    } else {
                        ++$fallbackCounts[$slug][$date];
                    }

                    $fallbackOffset = $fallbackFirstOffsets[$slug] ?? self::UNKNOWN_OFFSET_SENTINEL;
                    if ($fallbackOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $fallbackOffset) {
                        $fallbackFirstOffsets[$slug] = $absoluteOffset;
                    }
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
            $slug = \substr($carry, self::PATH_PREFIX_OFFSET, $lineLength - self::PATH_FIXED_SUFFIX);
            $date = \substr($carry, $lineLength - self::DATE_TAIL_OFFSET, 10);
            $absoluteOffset = $carryOffset;

            $knownPathId = $knownSlugToId[$slug] ?? null;
            $knownDateId = $knownDateToId[$date] ?? null;

            if (\is_int($knownPathId) && \is_int($knownDateId)) {
                $denseIndex = $knownPathId * $knownDateCount + $knownDateId;
                ++$denseCounts[$denseIndex];

                $existingOffset = $knownFirstOffsets[$knownPathId];
                if ($existingOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $existingOffset) {
                    $knownFirstOffsets[$knownPathId] = $absoluteOffset;
                }
            } else {
                if (\is_int($knownPathId)) {
                    $existingOffset = $knownFirstOffsets[$knownPathId];
                    if ($existingOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $existingOffset) {
                        $knownFirstOffsets[$knownPathId] = $absoluteOffset;
                    }
                }

                if (!isset($fallbackCounts[$slug][$date])) {
                    $fallbackCounts[$slug][$date] = 1;
                } else {
                    ++$fallbackCounts[$slug][$date];
                }

                $fallbackOffset = $fallbackFirstOffsets[$slug] ?? self::UNKNOWN_OFFSET_SENTINEL;
                if ($fallbackOffset === self::UNKNOWN_OFFSET_SENTINEL || $absoluteOffset < $fallbackOffset) {
                    $fallbackFirstOffsets[$slug] = $absoluteOffset;
                }
            }
        }

        \fclose($input);

        $payload = $this->encodeWorkerPayload($denseCounts, $knownFirstOffsets, $fallbackCounts, $fallbackFirstOffsets);
        if ($payload === null) {
            return false;
        }

        return $this->writeAllToSocket($socket, $payload);
    }

    private function encodeWorkerPayload(
        array $denseCounts,
        array $knownFirstOffsets,
        array $fallbackCounts,
        array $fallbackFirstOffsets
    ): ?string {
        $denseCountBin = $this->packUInt32Array($denseCounts);
        $knownFirstBin = $this->packU64Offsets($knownFirstOffsets);
        $fallbackPayload = $this->encodeFallbackPayload($fallbackCounts, $fallbackFirstOffsets);
        if ($fallbackPayload === null) {
            return null;
        }

        [$fallbackMetaBin, $fallbackDataBin] = $fallbackPayload;

        $header = \pack(
            'V4',
            \strlen($denseCountBin),
            \strlen($knownFirstBin),
            \strlen($fallbackMetaBin),
            \strlen($fallbackDataBin)
        );

        return $header . $denseCountBin . $knownFirstBin . $fallbackMetaBin . $fallbackDataBin;
    }

    private function packUInt32Array(array $values): string
    {
        if ($values === []) {
            return '';
        }

        $buffer = '';
        $chunk = [];
        $chunkCount = 0;
        $flushSize = 4096;

        foreach ($values as $value) {
            $chunk[] = $value;
            ++$chunkCount;

            if ($chunkCount === $flushSize) {
                $buffer .= \pack('V*', ...$chunk);
                $chunk = [];
                $chunkCount = 0;
            }
        }

        if ($chunkCount > 0) {
            $buffer .= \pack('V*', ...$chunk);
        }

        return $buffer;
    }

    private function packU64Offsets(array $offsets): string
    {
        $buffer = '';
        foreach ($offsets as $offset) {
            $buffer .= $this->packU64LE($offset);
        }

        return $buffer;
    }

    private function encodeFallbackPayload(array $fallbackCounts, array $fallbackFirstOffsets): ?array
    {
        $fallbackMetaBin = \pack('V', \count($fallbackFirstOffsets));
        foreach ($fallbackFirstOffsets as $slug => $offset) {
            $pathLength = \strlen($slug);
            if ($pathLength > 0xFFFF) {
                return null;
            }

            $fallbackMetaBin .= \pack('v', $pathLength) . $slug . $this->packU64LE($offset);
        }

        $recordCount = 0;
        foreach ($fallbackCounts as $dateCounts) {
            $recordCount += \count($dateCounts);
        }

        $fallbackDataBin = \pack('V', $recordCount);
        foreach ($fallbackCounts as $slug => $dateCounts) {
            $pathLength = \strlen($slug);
            if ($pathLength > 0xFFFF) {
                return null;
            }

            foreach ($dateCounts as $date => $count) {
                $dateLength = \strlen($date);
                if ($dateLength > 0xFFFF) {
                    return null;
                }

                $fallbackDataBin .= \pack('v', $pathLength) . $slug . \pack('v', $dateLength) . $date . \pack('V', $count);
            }
        }

        return [$fallbackMetaBin, $fallbackDataBin];
    }

    private function writeAllToSocket($socket, string $data): bool
    {
        $total = \strlen($data);
        $offset = 0;

        while ($offset < $total) {
            $chunk = \substr($data, $offset, self::READ_BLOCK_SIZE);
            $written = \fwrite($socket, $chunk);
            if (!\is_int($written) || $written <= 0) {
                return false;
            }

            $offset += $written;
        }

        return true;
    }

    private function readAllWorkerPayloads(array $parentSockets, array $socketToWorker): ?array
    {
        $payloads = \array_fill(0, self::WORKERS, '');
        $active = $parentSockets;

        while ($active !== []) {
            $read = \array_values($active);
            $write = null;
            $except = null;

            $selected = \stream_select($read, $write, $except, 5);
            if ($selected === false) {
                foreach ($active as $socket) {
                    \fclose($socket);
                }

                return null;
            }

            if ($selected === 0) {
                continue;
            }

            foreach ($read as $socket) {
                $worker = $socketToWorker[(int)$socket] ?? null;
                if (!\is_int($worker)) {
                    foreach ($active as $openSocket) {
                        \fclose($openSocket);
                    }

                    return null;
                }

                $chunk = \fread($socket, self::READ_BLOCK_SIZE);
                if ($chunk === false) {
                    foreach ($active as $openSocket) {
                        \fclose($openSocket);
                    }

                    return null;
                }

                if ($chunk === '') {
                    if (\feof($socket)) {
                        \fclose($socket);
                        unset($active[$worker]);
                    }

                    continue;
                }

                $payloads[$worker] .= $chunk;
            }
        }

        return $payloads;
    }

    private function mergeWorkerPayload(
        string $payload,
        int $denseArrayLength,
        int $knownPathCount,
        array &$globalDenseCounts,
        array &$globalKnownFirstOffsets,
        array &$globalFallbackCounts,
        array &$globalFallbackFirstOffsets
    ): bool {
        $decoded = $this->decodeWorkerPayload($payload, $denseArrayLength, $knownPathCount);
        if ($decoded === null) {
            return false;
        }

        [$denseCounts, $knownFirstOffsets, $fallbackCounts, $fallbackFirstOffsets] = $decoded;

        for ($i = 0; $i < $denseArrayLength; $i++) {
            $globalDenseCounts[$i] += $denseCounts[$i];
        }

        for ($i = 0; $i < $knownPathCount; $i++) {
            $workerOffset = $knownFirstOffsets[$i];
            if ($workerOffset === self::UNKNOWN_OFFSET_SENTINEL) {
                continue;
            }

            $globalOffset = $globalKnownFirstOffsets[$i];
            if ($globalOffset === self::UNKNOWN_OFFSET_SENTINEL || $workerOffset < $globalOffset) {
                $globalKnownFirstOffsets[$i] = $workerOffset;
            }
        }

        foreach ($fallbackFirstOffsets as $slug => $workerOffset) {
            $globalOffset = $globalFallbackFirstOffsets[$slug] ?? self::UNKNOWN_OFFSET_SENTINEL;
            if ($globalOffset === self::UNKNOWN_OFFSET_SENTINEL || $workerOffset < $globalOffset) {
                $globalFallbackFirstOffsets[$slug] = $workerOffset;
            }
        }

        foreach ($fallbackCounts as $slug => $dateCounts) {
            foreach ($dateCounts as $date => $count) {
                if (!isset($globalFallbackCounts[$slug][$date])) {
                    $globalFallbackCounts[$slug][$date] = $count;
                } else {
                    $globalFallbackCounts[$slug][$date] += $count;
                }
            }
        }

        return true;
    }

    private function decodeWorkerPayload(string $payload, int $denseArrayLength, int $knownPathCount): ?array
    {
        if (\strlen($payload) < 16) {
            return null;
        }

        $header = \unpack('VdenseLen/VfirstLen/VmetaLen/VdataLen', \substr($payload, 0, 16));
        if ($header === false) {
            return null;
        }

        $denseLen = $header['denseLen'];
        $firstLen = $header['firstLen'];
        $metaLen = $header['metaLen'];
        $dataLen = $header['dataLen'];

        $expectedLength = 16 + $denseLen + $firstLen + $metaLen + $dataLen;
        if (\strlen($payload) !== $expectedLength) {
            return null;
        }

        if ($denseLen !== $denseArrayLength * 4) {
            return null;
        }

        if ($firstLen !== $knownPathCount * 8) {
            return null;
        }

        $offset = 16;

        $denseBin = \substr($payload, $offset, $denseLen);
        $offset += $denseLen;

        $knownFirstBin = \substr($payload, $offset, $firstLen);
        $offset += $firstLen;

        $fallbackMetaBin = \substr($payload, $offset, $metaLen);
        $offset += $metaLen;

        $fallbackDataBin = \substr($payload, $offset, $dataLen);

        $denseCountsRaw = \unpack('V*', $denseBin);
        if ($denseCountsRaw === false || \count($denseCountsRaw) !== $denseArrayLength) {
            return null;
        }

        $denseCounts = [];
        for ($i = 1; $i <= $denseArrayLength; $i++) {
            $denseCounts[] = $denseCountsRaw[$i];
        }

        $knownFirstOffsets = $this->decodeU64Offsets($knownFirstBin, $knownPathCount);
        if ($knownFirstOffsets === null) {
            return null;
        }

        $fallbackDecoded = $this->decodeFallbackPayload($fallbackMetaBin, $fallbackDataBin);
        if ($fallbackDecoded === null) {
            return null;
        }

        [$fallbackFirstOffsets, $fallbackCounts] = $fallbackDecoded;

        return [$denseCounts, $knownFirstOffsets, $fallbackCounts, $fallbackFirstOffsets];
    }

    private function decodeU64Offsets(string $binary, int $count): ?array
    {
        if (\strlen($binary) !== $count * 8) {
            return null;
        }

        $offsets = [];
        $cursor = 0;
        for ($i = 0; $i < $count; $i++) {
            $chunk = \substr($binary, $cursor, 8);
            $value = $this->unpackU64LE($chunk);
            if (!\is_int($value)) {
                return null;
            }

            $offsets[] = $value;
            $cursor += 8;
        }

        return $offsets;
    }

    private function decodeFallbackPayload(string $metaBin, string $dataBin): ?array
    {
        $metaOffset = 0;
        $metaRecordCount = $this->readUInt32LE($metaBin, $metaOffset);
        if (!\is_int($metaRecordCount)) {
            return null;
        }

        $fallbackFirstOffsets = [];
        for ($i = 0; $i < $metaRecordCount; $i++) {
            $pathLength = $this->readUInt16LE($metaBin, $metaOffset);
            if (!\is_int($pathLength)) {
                return null;
            }

            if ($metaOffset + $pathLength + 8 > \strlen($metaBin)) {
                return null;
            }

            $slug = \substr($metaBin, $metaOffset, $pathLength);
            $metaOffset += $pathLength;

            $offset = $this->unpackU64LE(\substr($metaBin, $metaOffset, 8));
            if (!\is_int($offset)) {
                return null;
            }
            $metaOffset += 8;

            $fallbackFirstOffsets[$slug] = $offset;
        }

        if ($metaOffset !== \strlen($metaBin)) {
            return null;
        }

        $dataOffset = 0;
        $dataRecordCount = $this->readUInt32LE($dataBin, $dataOffset);
        if (!\is_int($dataRecordCount)) {
            return null;
        }

        $fallbackCounts = [];
        for ($i = 0; $i < $dataRecordCount; $i++) {
            $pathLength = $this->readUInt16LE($dataBin, $dataOffset);
            if (!\is_int($pathLength)) {
                return null;
            }

            if ($dataOffset + $pathLength > \strlen($dataBin)) {
                return null;
            }

            $slug = \substr($dataBin, $dataOffset, $pathLength);
            $dataOffset += $pathLength;

            $dateLength = $this->readUInt16LE($dataBin, $dataOffset);
            if (!\is_int($dateLength)) {
                return null;
            }

            if ($dataOffset + $dateLength > \strlen($dataBin)) {
                return null;
            }

            $date = \substr($dataBin, $dataOffset, $dateLength);
            $dataOffset += $dateLength;

            $count = $this->readUInt32LE($dataBin, $dataOffset);
            if (!\is_int($count)) {
                return null;
            }

            if (!isset($fallbackCounts[$slug][$date])) {
                $fallbackCounts[$slug][$date] = $count;
            } else {
                $fallbackCounts[$slug][$date] += $count;
            }
        }

        if ($dataOffset !== \strlen($dataBin)) {
            return null;
        }

        return [$fallbackFirstOffsets, $fallbackCounts];
    }

    private function readUInt16LE(string $binary, int &$offset): ?int
    {
        if ($offset + 2 > \strlen($binary)) {
            return null;
        }

        $value = \unpack('vvalue', \substr($binary, $offset, 2));
        if ($value === false) {
            return null;
        }

        $offset += 2;

        return $value['value'];
    }

    private function readUInt32LE(string $binary, int &$offset): ?int
    {
        if ($offset + 4 > \strlen($binary)) {
            return null;
        }

        $value = \unpack('Vvalue', \substr($binary, $offset, 4));
        if ($value === false) {
            return null;
        }

        $offset += 4;

        return $value['value'];
    }

    private function packU64LE(int $value): string
    {
        if ($value < 0) {
            return \pack('V2', 0xFFFFFFFF, 0xFFFFFFFF);
        }

        $low = $value & 0xFFFFFFFF;
        $high = ($value >> 32) & 0xFFFFFFFF;

        return \pack('V2', $low, $high);
    }

    private function unpackU64LE(string $bytes): ?int
    {
        if (\strlen($bytes) !== 8) {
            return null;
        }

        $parts = \unpack('Vlow/Vhigh', $bytes);
        if ($parts === false) {
            return null;
        }

        if ($parts['low'] === 0xFFFFFFFF && $parts['high'] === 0xFFFFFFFF) {
            return self::UNKNOWN_OFFSET_SENTINEL;
        }

        return ($parts['high'] << 32) | $parts['low'];
    }

    private function writeOutput(
        string $outputPath,
        array $knownSlugToId,
        array $knownIdToSlug,
        array $knownIdToDate,
        int $knownDateCount,
        array $globalDenseCounts,
        array $globalKnownFirstOffsets,
        array $globalFallbackCounts,
        array $globalFallbackFirstOffsets
    ): bool {
        $pathFirstOffsets = [];

        $knownPathCount = \count($knownIdToSlug);
        for ($pathId = 0; $pathId < $knownPathCount; $pathId++) {
            $offset = $globalKnownFirstOffsets[$pathId] ?? self::UNKNOWN_OFFSET_SENTINEL;
            if ($offset === self::UNKNOWN_OFFSET_SENTINEL) {
                continue;
            }

            $slug = $knownIdToSlug[$pathId];
            $pathFirstOffsets[$slug] = $offset;
        }

        foreach ($globalFallbackFirstOffsets as $slug => $offset) {
            $existing = $pathFirstOffsets[$slug] ?? self::UNKNOWN_OFFSET_SENTINEL;
            if ($existing === self::UNKNOWN_OFFSET_SENTINEL || $offset < $existing) {
                $pathFirstOffsets[$slug] = $offset;
            }
        }

        \asort($pathFirstOffsets, \SORT_NUMERIC);

        $output = \fopen($outputPath, 'wb');
        if ($output === false) {
            return false;
        }

        \stream_set_write_buffer($output, self::READ_BLOCK_SIZE);
        \fwrite($output, '{');

        $firstPath = true;
        foreach ($pathFirstOffsets as $slug => $_offset) {
            $dateMap = [];

            $knownPathId = $knownSlugToId[$slug] ?? null;
            if (\is_int($knownPathId)) {
                $base = $knownPathId * $knownDateCount;
                for ($dateId = 0; $dateId < $knownDateCount; $dateId++) {
                    $count = $globalDenseCounts[$base + $dateId];
                    if ($count > 0) {
                        $date = $knownIdToDate[$dateId];
                        $dateMap[$date] = $count;
                    }
                }
            }

            if (isset($globalFallbackCounts[$slug])) {
                foreach ($globalFallbackCounts[$slug] as $date => $count) {
                    if (!isset($dateMap[$date])) {
                        $dateMap[$date] = $count;
                    } else {
                        $dateMap[$date] += $count;
                    }
                }
            }

            if ($dateMap !== []) {
                \ksort($dateMap, \SORT_STRING);
            }

            $pathKey = \json_encode('/blog/' . $slug);
            if (!\is_string($pathKey)) {
                \fclose($output);

                return false;
            }

            if ($firstPath) {
                $firstPath = false;
            } else {
                \fwrite($output, ',');
            }

            \fwrite($output, "\n    {$pathKey}: {");

            $firstDate = true;
            foreach ($dateMap as $date => $count) {
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
