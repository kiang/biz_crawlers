#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Usage: php crawl-gcis.php <type> <year> <month> [options]\n";
    echo "Types:\n";
    echo "  company    - Crawl company data\n";
    echo "  business   - Crawl business data\n";
    echo "\n";
    echo "Options:\n";
    echo "  --enable-logs       Enable logging (logs disabled by default)\n";
    echo "  --verbose           Enable verbose logging (implies --enable-logs)\n";
    echo "\n";
    echo "Examples:\n";
    echo "  php crawl-gcis.php company 2024 6\n";
    echo "  php crawl-gcis.php business 2024 6 --enable-logs\n";
    echo "  php crawl-gcis.php company 2024 6 --verbose\n";
    exit(1);
}

if ($argc < 4) {
    showUsage();
}

$type = $argv[1];
$year = intval($argv[2]);
$month = intval($argv[3]);

// Parse additional options
$config = [];
for ($i = 4; $i < $argc; $i++) {
    switch ($argv[$i]) {
        case '--enable-logs':
            $config['enable_logging'] = true;
            break;
        case '--verbose':
            $config['enable_logging'] = true;
            $config['log_level'] = \Monolog\Logger::DEBUG;
            break;
        default:
            echo "Error: Unknown option '{$argv[$i]}'\n";
            showUsage();
    }
}

if (!in_array($type, ['company', 'business'])) {
    echo "Error: Invalid type '{$type}'. Must be 'company' or 'business'.\n";
    showUsage();
}

if ($year < 1900 || $year > 2100) {
    echo "Error: Invalid year '{$year}'.\n";
    showUsage();
}

if ($month < 1 || $month > 12) {
    echo "Error: Invalid month '{$month}'.\n";
    showUsage();
}

try {
    echo "Starting GCIS {$type} crawl for {$year}-{$month}\n";
    
    $config['log_file'] = "gcis_{$type}_{$year}_{$month}.log";
    $factory = new CrawlerFactory($config);
    
    $crawler = $factory->createGCISCrawler();
    
    if ($type === 'company') {
        $ids = $crawler->crawl(['year' => $year, 'month' => $month]);
    } else {
        $ids = $crawler->crawlBusiness($year, $month);
    }
    
    // Save IDs to data repository
    $idsFile = $crawler->saveIdList($type === 'company' ? 'companies' : 'businesses', $year, $month, $ids);
    
    echo "Crawl completed successfully!\n";
    echo "Found " . count($ids) . " unique IDs\n";
    echo "IDs saved to: {$idsFile}\n";
    
    // Output sample IDs
    if (!empty($ids)) {
        echo "Sample IDs:\n";
        $sampleIds = array_slice($ids, 0, 10);
        foreach ($sampleIds as $id) {
            echo "  {$id}\n";
        }
        if (count($ids) > 10) {
            echo "  ... and " . (count($ids) - 10) . " more\n";
        }
    }
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}