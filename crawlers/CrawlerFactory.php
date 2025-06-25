<?php

namespace BizData\Crawlers;

class CrawlerFactory
{
    private array $defaultConfig;

    public function __construct(array $config = [])
    {
        $this->defaultConfig = array_merge([
            'timeout' => 30,
            'delay' => 1,
            'retries' => 3,
            'user_agent' => 'BizData Crawler/1.0',
            'proxy' => getenv('PROXY_URL') ?: null,
            'log_file' => 'crawler.log',
            'log_level' => \Monolog\Logger::INFO
        ], $config);
    }

    public function createGCISCrawler(array $config = []): GCISCrawler
    {
        $mergedConfig = array_merge($this->defaultConfig, $config);
        return new GCISCrawler($mergedConfig);
    }

    public function createTaxCrawler(array $config = []): TaxCrawler
    {
        $mergedConfig = array_merge($this->defaultConfig, $config);
        return new TaxCrawler($mergedConfig);
    }

    public function createSchoolCrawler(array $config = []): SchoolCrawler
    {
        $mergedConfig = array_merge($this->defaultConfig, $config);
        return new SchoolCrawler($mergedConfig);
    }

    public function createDetailCrawler(array $config = []): DetailCrawler
    {
        $mergedConfig = array_merge($this->defaultConfig, $config);
        return new DetailCrawler($mergedConfig);
    }

    public function createCrawler(string $type, array $config = []): BaseCrawler
    {
        switch (strtolower($type)) {
            case 'gcis':
            case 'company':
                return $this->createGCISCrawler($config);

            case 'tax':
            case 'fia':
                return $this->createTaxCrawler($config);

            case 'school':
            case 'education':
                return $this->createSchoolCrawler($config);

            case 'detail':
            case 'details':
                return $this->createDetailCrawler($config);

            default:
                throw new \InvalidArgumentException("Unknown crawler type: {$type}");
        }
    }

    public static function getAvailableTypes(): array
    {
        return [
            'gcis' => 'GCIS Company/Business Crawler',
            'tax' => 'Ministry of Finance Tax Data Crawler',
            'school' => 'Ministry of Education School Crawler',
            'detail' => 'Company/Business Detail Crawler'
        ];
    }
}
