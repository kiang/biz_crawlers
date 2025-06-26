<?php

namespace BizData\Crawlers;

use Goutte\Client;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Symfony\Component\DomCrawler\Crawler;
use Symfony\Component\HttpClient\HttpClient;

abstract class BaseCrawler
{
    protected Client $client;
    protected Logger $logger;
    protected array $config;

    public function __construct(array $config = [])
    {
        $this->config = array_merge($this->getDefaultConfig(), $config);
        $this->initializeClient();
        $this->initializeLogger();
    }

    protected function getDefaultConfig(): array
    {
        return [
            'timeout' => 30,
            'delay' => 1,
            'retries' => 3,
            'user_agent' => 'BizData Crawler/1.0',
            'proxy' => null,
            'log_file' => 'crawler.log',
            'log_level' => Logger::INFO,
            'enable_logging' => false
        ];
    }

    protected function initializeClient(): void
    {
        $options = [
            'timeout' => $this->config['timeout'],
            'headers' => [
                'User-Agent' => $this->config['user_agent']
            ]
        ];

        if ($this->config['proxy']) {
            $options['proxy'] = $this->config['proxy'];
        }

        $httpClient = HttpClient::create($options);
        $this->client = new Client($httpClient);
    }

    protected function initializeLogger(): void
    {
        $this->logger = new Logger('crawler');
        if ($this->config['enable_logging']) {
            $handler = new StreamHandler($this->config['log_file'], $this->config['log_level']);
            $this->logger->pushHandler($handler);
        }
    }

    protected function fetch(string $url): Crawler
    {
        $attempts = 0;
        $maxAttempts = $this->config['retries'];

        while ($attempts < $maxAttempts) {
            try {
                $this->logger->info("Fetching URL: {$url}");

                if ($this->config['delay'] > 0) {
                    sleep($this->config['delay']);
                }

                $crawler = $this->client->request('GET', $url);
                $this->logger->info("Successfully fetched: {$url}");

                return $crawler;
            } catch (\Exception $e) {
                $attempts++;
                $this->logger->warning("Attempt {$attempts} failed for {$url}: " . $e->getMessage());

                if ($attempts >= $maxAttempts) {
                    $this->logger->error("Failed to fetch {$url} after {$maxAttempts} attempts");
                    throw new \Exception("Failed to fetch {$url}: " . $e->getMessage());
                }

                sleep(pow(2, $attempts)); // Exponential backoff
            }
        }

        throw new \Exception("Unexpected error fetching {$url}");
    }

    protected function downloadFile(string $url, string $destination = null): string
    {
        if (!$destination) {
            $destination = tempnam(sys_get_temp_dir(), 'crawler_');
        }

        $attempts = 0;
        $maxAttempts = $this->config['retries'];

        while ($attempts < $maxAttempts) {
            try {
                $this->logger->info("Downloading file: {$url}");

                if ($this->config['delay'] > 0) {
                    sleep($this->config['delay']);
                }

                // Use the same client to maintain session cookies
                $crawler = $this->client->request('GET', $url);
                $response = $this->client->getResponse();
                file_put_contents($destination, $response->getContent());

                $this->logger->info("Successfully downloaded: {$url} to {$destination}");
                return $destination;
            } catch (\Exception $e) {
                $attempts++;
                $this->logger->warning("Download attempt {$attempts} failed for {$url}: " . $e->getMessage());

                if ($attempts >= $maxAttempts) {
                    $this->logger->error("Failed to download {$url} after {$maxAttempts} attempts");
                    throw new \Exception("Failed to download {$url}: " . $e->getMessage());
                }

                sleep(pow(2, $attempts));
            }
        }

        throw new \Exception("Unexpected error downloading {$url}");
    }

    protected function extractText(Crawler $crawler, string $selector): string
    {
        try {
            return trim($crawler->filter($selector)->text());
        } catch (\Exception $e) {
            $this->logger->warning("Failed to extract text with selector '{$selector}': " . $e->getMessage());
            return '';
        }
    }

    protected function extractAttribute(Crawler $crawler, string $selector, string $attribute): string
    {
        try {
            return trim($crawler->filter($selector)->attr($attribute));
        } catch (\Exception $e) {
            $this->logger->warning("Failed to extract attribute '{$attribute}' with selector '{$selector}': " . $e->getMessage());
            return '';
        }
    }

    protected function extractMultiple(Crawler $crawler, string $selector): array
    {
        $results = [];

        try {
            $crawler->filter($selector)->each(function (Crawler $node) use (&$results) {
                $results[] = trim($node->text());
            });
        } catch (\Exception $e) {
            $this->logger->warning("Failed to extract multiple elements with selector '{$selector}': " . $e->getMessage());
        }

        return $results;
    }

    protected function saveDataToRepository(string $source, string $type, string $category, int $year, int $month, array $ids): string
    {
        $dataDir = dirname(__DIR__) . '/data';
        $sourceDir = "{$dataDir}/{$source}";

        if ($type) {
            $sourceDir .= "/{$type}";
        }

        $yearMonthDir = sprintf("%s/%03d-%02d", $sourceDir, $year, $month);

        if (!is_dir($yearMonthDir)) {
            mkdir($yearMonthDir, 0755, true);
        }

        $filename = "{$category}.json";
        $filepath = "{$yearMonthDir}/{$filename}";

        $data = [
            'metadata' => [
                'source' => $source,
                'type' => $type,
                'category' => $category,
                'year' => $year,
                'month' => $month,
                'crawled_at' => date('c'),
                'total_records' => count($ids)
            ],
            'data' => array_map(function ($id) {
                return ['id' => $id];
            }, array_values(array_unique($ids)))
        ];

        file_put_contents($filepath, json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
        $this->logger->info("Saved " . count($ids) . " records to {$filepath}");

        return $filepath;
    }

    protected function checkReportExists(string $source, string $type, string $category, int $year, int $month): bool
    {
        $dataDir = dirname(__DIR__) . '/data';
        $sourceDir = "{$dataDir}/{$source}";

        if ($type) {
            $sourceDir .= "/{$type}";
        }

        $yearMonthDir = sprintf("%s/%03d-%02d", $sourceDir, $year, $month);
        $filename = "{$category}.json";
        $filepath = "{$yearMonthDir}/{$filename}";

        return file_exists($filepath);
    }

    protected function loadExistingData(string $source, string $type, string $category, int $year, int $month): array
    {
        $dataDir = dirname(__DIR__) . '/data';
        $sourceDir = "{$dataDir}/{$source}";

        if ($type) {
            $sourceDir .= "/{$type}";
        }

        $yearMonthDir = sprintf("%s/%03d-%02d", $sourceDir, $year, $month);
        $filename = "{$category}.json";
        $filepath = "{$yearMonthDir}/{$filename}";

        if (!file_exists($filepath)) {
            return [];
        }

        $data = json_decode(file_get_contents($filepath), true);
        if (!$data || !isset($data['data'])) {
            return [];
        }

        return array_column($data['data'], 'id');
    }

    protected function saveIdsToDataRepository(string $source, string $type, int $year, int $month, array $ids): string
    {
        $dataDir = dirname(__DIR__) . '/data';
        $sourceDir = "{$dataDir}/{$source}";

        if ($type) {
            $sourceDir .= "/{$type}";
        }

        $yearMonthDir = sprintf("%s/%03d-%02d", $sourceDir, $year, $month);

        if (!is_dir($yearMonthDir)) {
            mkdir($yearMonthDir, 0755, true);
        }

        $filename = "ids_{$type}_{$year}_{$month}.txt";
        $filepath = "{$yearMonthDir}/{$filename}";

        // Only create the file if there are IDs to save
        if (!empty($ids)) {
            file_put_contents($filepath, implode("\n", $ids));
        }

        return $filepath;
    }

    abstract public function crawl(array $params = []): array;
}
