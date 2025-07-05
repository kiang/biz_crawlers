#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

function showUsage() {
    echo "Temple Business Data Crawler\n";
    echo "============================\n\n";
    echo "Usage: php crawl-temple.php [command] [options]\n\n";
    
    echo "Commands:\n";
    echo "  extract     - Process temple data from XML file\n";
    echo "  all         - Run extract command (default)\n\n";
    
    echo "Options:\n";
    echo "  --enable-logs           Enable logging\n";
    echo "  --verbose               Enable verbose logging\n";
    echo "  --help                  Show this help message\n\n";
    
    echo "Examples:\n";
    echo "  php crawl-temple.php                    # Extract all temple data\n";
    echo "  php crawl-temple.php extract --verbose   # Extract with verbose logging\n\n";
    
    exit(1);
}

$config = [
    'enable_logging' => false,
    'log_level' => \Monolog\Logger::INFO
];

$command = 'all';

if ($argc > 1) {
    if ($argv[1] === '--help') {
        showUsage();
    }
    
    if (in_array($argv[1], ['extract', 'all'])) {
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

$dataPath = __DIR__ . '/data';
$xmlFile = '/home/kiang/public_html/religion/raw/temple.xml';

function processTempleData($xmlFile, $dataPath) {
    echo "\n=== Processing temple data from XML file ===\n";
    
    if (!file_exists($xmlFile)) {
        echo "Error: XML file not found at {$xmlFile}\n";
        return 0;
    }
    
    $processedCount = 0;
    $savedCount = 0;
    $skippedCount = 0;
    
    try {
        $xml = simplexml_load_file($xmlFile);
        
        if (!$xml) {
            echo "Error: Failed to parse XML file\n";
            return 0;
        }
        
        echo "Loading temple data from XML...\n";
        
        foreach ($xml->OpenData_3 as $temple) {
            $processedCount++;
            
            if (isset($temple->統一編號) && !empty($temple->統一編號)) {
                $unifiedNumber = trim((string)$temple->統一編號);
                
                if (preg_match('/^\d{8}$/', $unifiedNumber)) {
                    $templeData = [
                        'id' => $unifiedNumber,
                        'crawled_at' => date('Y-m-d H:i:s'),
                        '編號' => trim((string)$temple->編號),
                        '寺廟名稱' => trim((string)$temple->寺廟名稱),
                        '主祀神祇' => trim((string)$temple->主祀神祇),
                        '行政區' => trim((string)$temple->行政區),
                        '地址' => trim((string)$temple->地址),
                        '教別' => trim((string)$temple->教別),
                        '登記別' => trim((string)$temple->登記別),
                        '統一編號' => $unifiedNumber,
                        '電話' => trim((string)$temple->電話),
                        '負責人' => trim((string)$temple->負責人),
                        '其他' => trim((string)$temple->其他),
                        'WGS84X' => trim((string)$temple->WGS84X),
                        'WGS84Y' => trim((string)$temple->WGS84Y)
                    ];
                    
                    $firstDigit = substr($unifiedNumber, 0, 1);
                    $outputDir = $dataPath . "/others/{$firstDigit}";
                    if (!file_exists($outputDir)) {
                        mkdir($outputDir, 0777, true);
                    }
                    
                    $outputFile = $outputDir . "/{$unifiedNumber}.json";
                    
                    if (file_exists($outputFile)) {
                        $existingData = json_decode(file_get_contents($outputFile), true);
                        if ($existingData) {
                            foreach ($templeData as $field => $value) {
                                $existingData[$field] = $value;
                            }
                            $existingData['crawled_at'] = date('Y-m-d H:i:s');
                            $templeData = $existingData;
                        }
                    }
                    
                    file_put_contents($outputFile, json_encode($templeData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE));
                    $savedCount++;
                    
                    if ($savedCount % 100 == 0) {
                        echo "Processed {$savedCount} temples with 統一編號...\n";
                    }
                } else {
                    echo "Invalid 統一編號 format: {$unifiedNumber}\n";
                    $skippedCount++;
                }
            } else {
                $skippedCount++;
            }
        }
        
        echo "\nProcessing complete:\n";
        echo "- Total temples processed: {$processedCount}\n";
        echo "- Temples with valid 統一編號: {$savedCount}\n";
        echo "- Temples without 統一編號: {$skippedCount}\n";
        
    } catch (Exception $e) {
        echo "Error processing XML: " . $e->getMessage() . "\n";
        return 0;
    }
    
    return $savedCount;
}

try {
    if ($config['enable_logging']) {
        $config['log_file'] = "temple_crawl_" . date('Y-m-d_H-i-s') . '.log';
    }
    
    echo "Temple Business Data Crawler\n";
    echo "Command: {$command}\n";
    echo "XML File: {$xmlFile}\n\n";
    
    switch ($command) {
        case 'extract':
        case 'all':
        default:
            processTempleData($xmlFile, $dataPath);
            break;
    }
    
    echo "\nProcessing completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}