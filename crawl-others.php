#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Dataset Business Data Crawler\n";
    echo "============================\n\n";
    echo "Usage: php crawl-others.php [command] [options]\n\n";
    
    echo "Commands:\n";
    echo "  download    - Download CSV data from datasets\n";
    echo "  extract     - Process business data from downloaded CSVs\n";
    echo "  all         - Run all commands in sequence (default)\n\n";
    
    echo "Options:\n";
    echo "  --datasets <ids>        Comma-separated dataset IDs (default: 44806,75136,128650,41400,161280,121974)\n";
    echo "  --enable-logs           Enable logging\n";
    echo "  --verbose               Enable verbose logging\n";
    echo "  --help                  Show this help message\n\n";
    
    echo "Examples:\n";
    echo "  php crawl-others.php                                  # Run all commands with defaults\n";
    echo "  php crawl-others.php download --datasets 44806,75136  # Download specific datasets\n";
    echo "  php crawl-others.php all --verbose                    # Full run with verbose logging\n\n";
    
    exit(1);
}

// Default configuration
$config = [
    'datasets' => [44806, 75136, 128650, 41400, 161280, 121974],
    'enable_logging' => false,
    'log_level' => \Monolog\Logger::INFO
];

$command = 'all';

// Parse command line arguments
if ($argc > 1) {
    if ($argv[1] === '--help') {
        showUsage();
    }
    
    if (in_array($argv[1], ['download', 'extract', 'all'])) {
        $command = $argv[1];
        $startIndex = 2;
    } else {
        $startIndex = 1;
    }
    
    for ($i = $startIndex; $i < $argc; $i++) {
        switch ($argv[$i]) {
            case '--help':
                showUsage();
                break;
                
            case '--datasets':
                if (!isset($argv[$i + 1])) {
                    echo "Error: --datasets requires a comma-separated list of IDs\n";
                    exit(1);
                }
                $config['datasets'] = array_map('intval', explode(',', $argv[$i + 1]));
                $i++;
                break;
                
            
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
}

$rawPath = __DIR__ . '/raw';
$dataPath = __DIR__ . '/data';

function downloadDatasets($datasets, $rawPath) {
    echo "=== Downloading CSV datasets ===\n";
    
    foreach($datasets as $datasetId) {
        echo "Processing dataset ID: {$datasetId}\n";
        
        $json = json_decode(file_get_contents("https://data.gov.tw/api/v2/rest/dataset/{$datasetId}"), true);
        
        if(!isset($json['result']['distribution'])) {
            echo "No distributions found for dataset {$datasetId}\n";
            continue;
        }
        
        foreach($json['result']['distribution'] as $item) {
            if($item['resourceFormat'] === 'CSV') {
                $targetPath = $rawPath . "/dataset_{$datasetId}";
                if (!file_exists($targetPath)) {
                    mkdir($targetPath, 0777, true);
                }
                
                $targetFile = $targetPath . '/data.csv';
                if(!file_exists($targetFile)) {
                    echo "Downloading CSV for dataset {$datasetId}...\n";
                    $content = shell_exec("curl '{$item['resourceDownloadUrl']}' -H 'Host: data.moi.gov.tw' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Connection: keep-alive' -H 'Upgrade-Insecure-Requests: 1'");
                    if(!empty($content)) {
                        file_put_contents($targetFile, $content);
                        echo "Saved CSV for dataset {$datasetId}\n";
                    } else {
                        echo "Failed to download CSV for dataset {$datasetId}\n";
                    }
                } else {
                    echo "CSV already exists for dataset {$datasetId}\n";
                }
                break; // Only process first CSV distribution
            }
        }
    }
}

function processBusinessData($datasets, $rawPath, $dataPath) {
    echo "\n=== Processing business data from CSV files ===\n";
    
    $processedCount = 0;
    $savedCount = 0;
    
    foreach($datasets as $datasetId) {
        $csvFile = $rawPath . "/dataset_{$datasetId}/data.csv";
        if(file_exists($csvFile)) {
            echo "Processing CSV for dataset {$datasetId}...\n";
            
            $handle = fopen($csvFile, 'r');
            if($handle) {
                // Clean BOM and read header
                $header = fgetcsv($handle);
                if($header && isset($header[0])) {
                    $header[0] = ltrim($header[0], "\xEF\xBB\xBF"); // Remove BOM
                }
                
                $unifiedNumberCol = -1;
                
                // Find 統一編號 column
                foreach($header as $index => $col) {
                    if(strpos($col, '統一編號') !== false || strpos($col, '統編') !== false) {
                        $unifiedNumberCol = $index;
                        break;
                    }
                }
                
                if($unifiedNumberCol >= 0) {
                    echo "Found 統一編號 column at index {$unifiedNumberCol} for dataset {$datasetId}\n";
                    
                    while(($row = fgetcsv($handle)) !== false) {
                        if(isset($row[$unifiedNumberCol]) && !empty($row[$unifiedNumberCol])) {
                            $unifiedNumber = trim($row[$unifiedNumberCol]);
                            if(preg_match('/^\d{8}$/', $unifiedNumber)) { // 8-digit unified number
                                $processedCount++;
                                
                                // Create business data structure
                                $currentDatasetData = [];
                                
                                // Add all available fields (including empty ones)
                                foreach($header as $index => $fieldName) {
                                    if(isset($row[$index])) {
                                        $currentDatasetData[trim($fieldName)] = trim($row[$index]);
                                    }
                                }
                                
                                // Save to file
                                $firstDigit = substr($unifiedNumber, 0, 1);
                                $outputDir = $dataPath . "/others/{$firstDigit}";
                                if (!file_exists($outputDir)) {
                                    mkdir($outputDir, 0777, true);
                                }
                                
                                $outputFile = $outputDir . "/{$unifiedNumber}.json";
                                
                                // Check if file exists and merge data if needed
                                if(file_exists($outputFile)) {
                                    $businessData = json_decode(file_get_contents($outputFile), true);
                                    if($businessData) {
                                        // Merge new fields into existing data
                                        foreach($currentDatasetData as $field => $value) {
                                            $businessData[$field] = $value;
                                        }
                                        $businessData['crawled_at'] = date('Y-m-d H:i:s');
                                    }
                                } else {
                                    $businessData = [
                                        'id' => $unifiedNumber,
                                        'crawled_at' => date('Y-m-d H:i:s')
                                    ];
                                    // Add all fields to top level
                                    foreach($currentDatasetData as $field => $value) {
                                        $businessData[$field] = $value;
                                    }
                                }
                                
                                file_put_contents($outputFile, json_encode($businessData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
                                $savedCount++;
                            }
                        }
                    }
                } else {
                    echo "No 統一編號 column found in dataset {$datasetId}\n";
                }
                
                fclose($handle);
            }
        }
    }
    
    echo "Processed {$processedCount} records, saved {$savedCount} JSON files\n";
    
    return $savedCount;
}


try {
    if ($config['enable_logging']) {
        $config['log_file'] = "dataset_crawl_" . date('Y-m-d_H-i-s') . '.log';
    }
    
    echo "Dataset Business Data Crawler\n";
    echo "Command: {$command}\n";
    echo "Datasets: " . implode(', ', $config['datasets']) . "\n\n";
    
    $unifiedNumbers = [];
    
    switch ($command) {
        case 'download':
            downloadDatasets($config['datasets'], $rawPath);
            break;
            
        case 'extract':
            processBusinessData($config['datasets'], $rawPath, $dataPath);
            break;
            
        
        case 'all':
        default:
            downloadDatasets($config['datasets'], $rawPath);
            processBusinessData($config['datasets'], $rawPath, $dataPath);
            break;
    }
    
    echo "\nProcessing completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}