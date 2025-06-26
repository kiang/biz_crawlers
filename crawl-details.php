#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Usage: php crawl-details.php <type> <source> [options]\n";
    echo "\n";
    echo "Types:\n";
    echo "  company    - Crawl company details\n";
    echo "  business   - Crawl business details\n";
    echo "\n";
    echo "Source:\n";
    echo "  --ids <id1,id2,id3>     Comma-separated list of IDs\n";
    echo "  --file <filename>       File containing IDs (one per line)\n";
    echo "  --from-data <year> <month>      Load IDs from data repository\n";
    echo "  --from-json             Load all IDs from existing JSON files\n";
    echo "\n";
    echo "Options:\n";
    echo "  --enable-logs           Enable logging (logs disabled by default)\n";
    echo "  --verbose               Enable verbose logging (implies --enable-logs)\n";
    echo "  --safe                  Enable safe mode (slower but more stable, overrides default fast mode)\n";
    echo "  --limit <number>        Limit number of IDs to process\n";
    echo "  --offset <number>       Skip first N IDs\n";
    echo "  --help                  Show this help message\n";
    echo "\n";
    echo "Examples:\n";
    echo "  php crawl-details.php company --ids 12345678,87654321\n";
    echo "  php crawl-details.php business --file business_ids.txt --enable-logs\n";
    echo "  php crawl-details.php company --from-data 2025 4 --limit 100\n";
    echo "  php crawl-details.php company --from-data 2025 4 --limit 10 --safe\n";
    echo "  php crawl-details.php company --from-json --limit 100\n";
    exit(1);
}

// Parse command line arguments
if ($argc < 3) {
    showUsage();
}

$type = $argv[1];
if (!in_array($type, ['company', 'business'])) {
    echo "Error: Invalid type '{$type}'. Must be 'company' or 'business'.\n";
    showUsage();
}

$options = [];
$config = [];
$ids = [];

for ($i = 2; $i < $argc; $i++) {
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
            
        case '--safe':
            $config['fast_mode'] = false;
            $config['rate_limit'] = 0.5;
            $config['retry_delay'] = 10;
            $config['max_retries'] = 3;
            $config['session_init_delay'] = 2;
            $config['search_delay'] = 5;
            break;
            
        case '--ids':
            if (!isset($argv[$i + 1])) {
                echo "Error: --ids requires a comma-separated list of IDs\n";
                showUsage();
            }
            $ids = array_filter(explode(',', $argv[$i + 1]));
            $i++;
            break;
            
        case '--file':
            if (!isset($argv[$i + 1])) {
                echo "Error: --file requires a filename\n";
                showUsage();
            }
            $filename = $argv[$i + 1];
            if (!file_exists($filename)) {
                echo "Error: File '{$filename}' does not exist\n";
                exit(1);
            }
            $ids = array_filter(array_map('trim', file($filename)));
            $i++;
            break;
            
        case '--from-data':
            if (!isset($argv[$i + 2])) {
                echo "Error: --from-data requires year and month\n";
                showUsage();
            }
            $year = intval($argv[$i + 1]);
            $month = intval($argv[$i + 2]);
            // Auto-determine dataType from crawler type
            $dataType = ($type === 'company') ? 'companies' : 'businesses';
            
            $dataDir = __DIR__ . "/data/gcis/{$dataType}";
            $yearMonthDir = sprintf("%s/%03d-%02d", $dataDir, $year, $month);
            $idsFile = "{$yearMonthDir}/ids_{$dataType}_{$year}_{$month}.txt";
            
            if (!file_exists($idsFile)) {
                echo "IDs file '{$idsFile}' does not exist. Generating it...\n";
                
                // Convert plural form to singular for crawl-gcis.php
                $gcisType = ($dataType === 'companies') ? 'company' : 'business';
                
                // Run crawl-gcis.php to generate the ID file
                $gcisScript = __DIR__ . '/crawl-gcis.php';
                $command = "php {$gcisScript} {$gcisType} {$year} {$month}";
                
                echo "Running: {$command}\n";
                $output = [];
                $returnCode = 0;
                exec($command, $output, $returnCode);
                
                if ($returnCode !== 0) {
                    echo "Error: Failed to generate IDs file. Command returned {$returnCode}\n";
                    echo "Output: " . implode("\n", $output) . "\n";
                    exit(1);
                }
                
                echo "Successfully generated IDs file\n";
                
                // Check if the file was created
                if (!file_exists($idsFile)) {
                    echo "Error: IDs file was not created after running crawl-gcis.php\n";
                    exit(1);
                }
            }
            
            $ids = array_filter(array_map('trim', file($idsFile)));
            echo "Loaded " . count($ids) . " IDs from {$idsFile}\n";
            $i += 2;
            break;
            
        case '--from-json':
            // Auto-determine dataType from crawler type
            $dataType = ($type === 'company') ? 'companies' : 'businesses';
            
            $dataDir = __DIR__ . "/data/gcis/{$dataType}/details";
            if (!is_dir($dataDir)) {
                echo "Error: Data directory '{$dataDir}' does not exist\n";
                exit(1);
            }
            
            echo "Scanning for existing JSON files in {$dataDir}...\n";
            
            // Scan all subdirectories for JSON files
            $jsonFiles = [];
            for ($digit = 0; $digit <= 9; $digit++) {
                $subDir = "{$dataDir}/{$digit}";
                if (is_dir($subDir)) {
                    $files = glob("{$subDir}/*.json");
                    $jsonFiles = array_merge($jsonFiles, $files);
                }
            }
            
            // Extract IDs from filenames
            $ids = [];
            foreach ($jsonFiles as $jsonFile) {
                $filename = basename($jsonFile, '.json');
                if (preg_match('/^\d{8}$/', $filename)) {
                    $ids[] = $filename;
                }
            }
            
            sort($ids);
            echo "Found " . count($ids) . " existing JSON files\n";
            break;
            
        case '--limit':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --limit requires a numeric value\n";
                exit(1);
            }
            $options['limit'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        case '--offset':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --offset requires a numeric value\n";
                exit(1);
            }
            $options['offset'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        default:
            echo "Error: Unknown option '{$argv[$i]}'\n";
            showUsage();
    }
}

if (empty($ids)) {
    echo "Error: No IDs specified. Use --ids, --file, or --from-data\n";
    showUsage();
}

// Apply offset and limit
if (isset($options['offset'])) {
    $ids = array_slice($ids, $options['offset']);
}

if (isset($options['limit'])) {
    $ids = array_slice($ids, 0, $options['limit']);
}

try {
    echo "Starting {$type} detail crawler\n";
    echo "Processing " . count($ids) . " IDs\n";
    
    $config['log_file'] = "detail_crawl_{$type}_" . date('Y-m-d_H-i-s') . '.log';
    $factory = new CrawlerFactory($config);
    $crawler = $factory->createDetailCrawler();
    
    $processed = 0;
    $successful = 0;
    $failed = 0;
    
    foreach ($ids as $id) {
        $id = trim($id);
        if (empty($id)) {
            continue;
        }
        
        $processed++;
        echo "Processing {$type} {$id} ({$processed}/" . count($ids) . ")... ";
        
        try {
            if ($type === 'company') {
                $data = $crawler->fetchCompanyDetail($id);
            } else {
                $data = $crawler->fetchBusinessDetail($id);
            }
            
            if ($data) {
                if ($type === 'company') {
                    $crawler->saveCompanyDetail($id, $data);
                } else {
                    $crawler->saveBusinessDetail($id, $data);
                }
                echo "SUCCESS\n";
                $successful++;
            } else {
                echo "NOT FOUND\n";
                $failed++;
            }
        } catch (Exception $e) {
            echo "ERROR: " . $e->getMessage() . "\n";
            $failed++;
        }
        
        // Progress report every 10 items
        if ($processed % 10 === 0) {
            echo "Progress: {$processed}/" . count($ids) . " processed, {$successful} successful, {$failed} failed\n";
        }
    }
    
    echo "\nCrawl completed!\n";
    echo "Total processed: {$processed}\n";
    echo "Successful: {$successful}\n";
    echo "Failed: {$failed}\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}