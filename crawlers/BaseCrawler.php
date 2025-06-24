<?php

namespace BizData\Crawlers;

use Goutte\Client;
use GuzzleHttp\Client as GuzzleClient;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Symfony\Component\DomCrawler\Crawler;

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
            'log_level' => Logger::INFO
        ];
    }
    
    protected function initializeClient(): void
    {
        $guzzleConfig = [
            'timeout' => $this->config['timeout'],
            'verify' => false,
            'headers' => [
                'User-Agent' => $this->config['user_agent']
            ]
        ];
        
        if ($this->config['proxy']) {
            $guzzleConfig['proxy'] = $this->config['proxy'];
        }
        
        $guzzleClient = new GuzzleClient($guzzleConfig);
        $this->client = new Client($guzzleClient);
    }
    
    protected function initializeLogger(): void
    {
        $this->logger = new Logger('crawler');
        $handler = new StreamHandler($this->config['log_file'], $this->config['log_level']);
        $this->logger->pushHandler($handler);
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
                
                $response = $this->client->getClient()->get($url);
                file_put_contents($destination, $response->getBody());
                
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
    
    abstract public function crawl(array $params = []): array;
}