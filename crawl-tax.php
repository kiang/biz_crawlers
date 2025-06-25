#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Usage: php crawl-tax.php [csv_file] [options]\n";
    echo "\n";
    echo "If no CSV file is provided, the script will download the latest data from FIA.\n";
    echo "\n";
    echo "Options:\n";
    echo "  --enable-logs       Enable logging (logs disabled by default)\n";
    echo "  --verbose           Enable verbose logging (implies --enable-logs)\n";
    echo "\n";
    echo "Examples:\n";
    echo "  php crawl-tax.php                    # Download latest data\n";
    echo "  php crawl-tax.php bgmopen1.csv       # Process existing CSV file\n";
    echo "  php crawl-tax.php --enable-logs      # Download with logging\n";
    echo "  php crawl-tax.php bgmopen1.csv --verbose  # Process with verbose logging\n";
    exit(1);
}

// Parse arguments
$csvFile = null;
$config = [];

for ($i = 1; $i < $argc; $i++) {
    if ($argv[$i] === '--enable-logs') {
        $config['enable_logging'] = true;
    } elseif ($argv[$i] === '--verbose') {
        $config['enable_logging'] = true;
        $config['log_level'] = \Monolog\Logger::DEBUG;
    } elseif (!$csvFile && !str_starts_with($argv[$i], '--')) {
        $csvFile = $argv[$i];
    } else {
        echo "Error: Unknown option '{$argv[$i]}'\n";
        showUsage();
    }
}

if ($csvFile && !file_exists($csvFile)) {
    echo "Error: CSV file '{$csvFile}' does not exist.\n";
    showUsage();
}

try {
    echo "Starting tax data crawl\n";
    
    if ($csvFile) {
        echo "Processing file: {$csvFile}\n";
    } else {
        echo "Downloading latest tax data from Ministry of Finance\n";
    }
    
    $config['log_file'] = 'tax_crawl_' . date('Y-m-d_H-i-s') . '.log';
    $factory = new CrawlerFactory($config);
    
    $crawler = $factory->createTaxCrawler();
    
    $params = [];
    if ($csvFile) {
        $params['file'] = $csvFile;
    }
    
    $result = $crawler->crawl($params);
    
    // Handle generator result for large files
    if (is_array($result) && isset($result['status'])) {
        if ($result['status'] === 'no_changes') {
            echo "Tax data file has not changed since last update.\n";
        } else {
            echo "Crawl completed successfully!\n";
            if (isset($result['lines_processed'])) {
                echo "Lines processed: " . $result['lines_processed'] . "\n";
            }
        }
    } else {
        // Handle batch processing
        $totalRecords = 0;
        $batchNumber = 1;
        
        foreach ($result as $batch) {
            $recordCount = count($batch);
            $totalRecords += $recordCount;
            
            echo "Processed batch {$batchNumber}: {$recordCount} records\n";
            
            // Here you would normally save the batch to database
            // For now, we'll just save a sample to demonstrate
            if ($batchNumber === 1) {
                $sampleFile = 'tax_sample_' . date('Y-m-d_H-i-s') . '.json';
                $sampleData = array_slice($batch, 0, 5);
                file_put_contents($sampleFile, json_encode($sampleData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
                echo "Sample data saved to: {$sampleFile}\n";
            }
            
            $batchNumber++;
        }
        
        echo "Total records processed: {$totalRecords}\n";
    }
    
    echo "Tax crawl completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}