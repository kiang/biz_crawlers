#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Business Data Crawler - Unified Interface\n";
    echo "=========================================\n\n";
    echo "Usage: php crawl.php <type> [options]\n\n";
    
    echo "Available Types:\n";
    $types = CrawlerFactory::getAvailableTypes();
    foreach ($types as $type => $description) {
        echo "  {$type} - {$description}\n";
    }
    
    echo "\nExamples:\n";
    echo "  php crawl.php gcis --company --year 2024 --month 6\n";
    echo "  php crawl.php gcis --business --year 2024 --month 6\n";
    echo "  php crawl.php tax\n";
    echo "  php crawl.php tax --file bgmopen1.csv\n";
    echo "  php crawl.php school\n";
    echo "  php crawl.php school --csv schools.csv\n";
    echo "  php crawl.php school --category \"大學\"\n";
    echo "  php crawl.php school --tax-id 12345678\n";
    echo "  php crawl.php detail --company --from-data 2025 4 companies --limit 10\n";
    echo "  php crawl.php detail --business --ids 12345678,87654321 --safe\n\n";
    
    echo "Global Options:\n";
    echo "  --help              Show this help message\n";
    echo "  --enable-logs       Enable logging (logs disabled by default)\n";
    echo "  --verbose           Enable verbose logging (implies --enable-logs)\n";
    echo "  --safe              Enable safe mode for detail crawler (slower but more stable)\n";
    echo "  --delay <seconds>   Delay between requests (default: 1)\n";
    echo "  --timeout <seconds> Request timeout (default: 30)\n";
    echo "  --retries <count>   Retry attempts (default: 3)\n\n";
    
    exit(1);
}

if ($argc < 2) {
    showUsage();
}

$type = $argv[1];

if ($type === '--help') {
    showUsage();
}

// Parse arguments
$options = [];
$globalOptions = [];

for ($i = 2; $i < $argc; $i++) {
    switch ($argv[$i]) {
        case '--help':
            showUsage();
            break;
            
        case '--enable-logs':
            $globalOptions['enable_logging'] = true;
            break;
            
        case '--verbose':
            $globalOptions['enable_logging'] = true;
            $globalOptions['log_level'] = \Monolog\Logger::DEBUG;
            break;
            
        case '--safe':
            $globalOptions['fast_mode'] = false;
            $globalOptions['rate_limit'] = 0.5;
            $globalOptions['retry_delay'] = 10;
            $globalOptions['max_retries'] = 3;
            $globalOptions['session_init_delay'] = 2;
            $globalOptions['search_delay'] = 5;
            break;
            
        case '--delay':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --delay requires a numeric value\n";
                exit(1);
            }
            $globalOptions['delay'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        case '--timeout':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --timeout requires a numeric value\n";
                exit(1);
            }
            $globalOptions['timeout'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        case '--retries':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --retries requires a numeric value\n";
                exit(1);
            }
            $globalOptions['retries'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        // Type-specific options
        case '--company':
            $options['mode'] = 'company';
            break;
            
        case '--business':
            $options['mode'] = 'business';
            break;
            
        case '--year':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --year requires a numeric value\n";
                exit(1);
            }
            $options['year'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        case '--month':
            if (!isset($argv[$i + 1]) || !is_numeric($argv[$i + 1])) {
                echo "Error: --month requires a numeric value\n";
                exit(1);
            }
            $options['month'] = intval($argv[$i + 1]);
            $i++;
            break;
            
        case '--file':
            if (!isset($argv[$i + 1])) {
                echo "Error: --file requires a filename\n";
                exit(1);
            }
            $options['file'] = $argv[$i + 1];
            $i++;
            break;
            
        case '--csv':
            if (!isset($argv[$i + 1])) {
                echo "Error: --csv requires a filename\n";
                exit(1);
            }
            $options['csv'] = $argv[$i + 1];
            $i++;
            break;
            
        case '--category':
            if (!isset($argv[$i + 1])) {
                echo "Error: --category requires a category name\n";
                exit(1);
            }
            $options['category'] = $argv[$i + 1];
            $i++;
            break;
            
        case '--tax-id':
            if (!isset($argv[$i + 1])) {
                echo "Error: --tax-id requires a tax ID\n";
                exit(1);
            }
            $options['tax_id'] = $argv[$i + 1];
            $i++;
            break;
            
        case '--ids':
            if (!isset($argv[$i + 1])) {
                echo "Error: --ids requires a comma-separated list of IDs\n";
                exit(1);
            }
            $options['ids'] = array_filter(explode(',', $argv[$i + 1]));
            $i++;
            break;
            
        case '--from-data':
            if (!isset($argv[$i + 3])) {
                echo "Error: --from-data requires year, month, and type\n";
                exit(1);
            }
            $options['from_data'] = [
                'year' => intval($argv[$i + 1]),
                'month' => intval($argv[$i + 2]),
                'type' => $argv[$i + 3]
            ];
            $i += 3;
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

try {
    $globalOptions['log_file'] = "{$type}_crawl_" . date('Y-m-d_H-i-s') . '.log';
    
    $factory = new CrawlerFactory($globalOptions);
    $crawler = $factory->createCrawler($type);
    
    echo "Starting {$type} crawler\n";
    
    switch ($type) {
        case 'gcis':
            if (!isset($options['mode'])) {
                echo "Error: GCIS crawler requires --company or --business mode\n";
                exit(1);
            }
            
            if (!isset($options['year']) || !isset($options['month'])) {
                echo "Error: GCIS crawler requires --year and --month\n";
                exit(1);
            }
            
            echo "Mode: {$options['mode']}, Year: {$options['year']}, Month: {$options['month']}\n";
            
            if ($options['mode'] === 'company') {
                $ids = $crawler->crawl(['year' => $options['year'], 'month' => $options['month']]);
                $idsFile = $crawler->saveIdList('companies', $options['year'], $options['month'], $ids);
            } else {
                $ids = $crawler->crawlBusiness($options['year'], $options['month']);
                $idsFile = $crawler->saveIdList('businesses', $options['year'], $options['month'], $ids);
            }
            
            echo "Found " . count($ids) . " unique IDs\n";
            echo "IDs saved to: {$idsFile}\n";
            break;
            
        case 'tax':
            $params = [];
            if (isset($options['file'])) {
                if (!file_exists($options['file'])) {
                    echo "Error: File '{$options['file']}' does not exist\n";
                    exit(1);
                }
                $params['file'] = $options['file'];
                echo "Processing file: {$options['file']}\n";
            } else {
                echo "Downloading latest tax data\n";
            }
            
            $result = $crawler->crawl($params);
            
            if (is_array($result) && isset($result['status'])) {
                if ($result['status'] === 'no_changes') {
                    echo "Tax data has not changed\n";
                } else {
                    echo "Processing completed\n";
                }
            }
            break;
            
        case 'school':
            if (isset($options['tax_id'])) {
                echo "Searching for tax ID: {$options['tax_id']}\n";
                $school = $crawler->getSchoolByTaxId($options['tax_id']);
                if ($school) {
                    echo "Found: {$school['name']} ({$school['category']})\n";
                } else {
                    echo "School not found\n";
                }
            } elseif (isset($options['category'])) {
                echo "Searching category: {$options['category']}\n";
                $schools = $crawler->getSchoolsByCategory($options['category']);
                echo "Found " . count($schools) . " schools\n";
                
                if (isset($options['csv'])) {
                    $handle = fopen($options['csv'], 'w');
                    fputcsv($handle, ['tax_id', 'name', 'category', 'source', 'type', 'crawled_at']);
                    foreach ($schools as $school) {
                        fputcsv($handle, array_values($school));
                    }
                    fclose($handle);
                    echo "Exported to: {$options['csv']}\n";
                }
            } else {
                $schools = $crawler->crawl();
                echo "Found " . count($schools) . " schools\n";
                
                if (isset($options['csv'])) {
                    $crawler->exportToCSV($options['csv']);
                    echo "Exported to: {$options['csv']}\n";
                }
            }
            break;
            
        case 'detail':
        case 'details':
            if (!isset($options['mode'])) {
                echo "Error: Detail crawler requires --company or --business mode\n";
                exit(1);
            }
            
            $ids = [];
            
            // Load IDs from various sources
            if (isset($options['ids'])) {
                $ids = $options['ids'];
            } elseif (isset($options['from_data'])) {
                $data = $options['from_data'];
                $dataDir = __DIR__ . "/data/gcis/{$data['type']}";
                $yearMonthDir = sprintf("%s/%03d-%02d", $dataDir, $data['year'], $data['month']);
                $idsFile = "{$yearMonthDir}/ids_{$data['type']}_{$data['year']}_{$data['month']}.txt";
                
                if (!file_exists($idsFile)) {
                    echo "Error: IDs file '{$idsFile}' does not exist\n";
                    exit(1);
                }
                
                $ids = array_filter(array_map('trim', file($idsFile)));
                echo "Loaded " . count($ids) . " IDs from {$idsFile}\n";
            } else {
                echo "Error: Detail crawler requires --ids or --from-data\n";
                exit(1);
            }
            
            // Apply offset and limit
            if (isset($options['offset'])) {
                $ids = array_slice($ids, $options['offset']);
            }
            
            if (isset($options['limit'])) {
                $ids = array_slice($ids, 0, $options['limit']);
            }
            
            echo "Processing " . count($ids) . " {$options['mode']} IDs\n";
            
            $processed = 0;
            $successful = 0;
            $failed = 0;
            
            foreach ($ids as $id) {
                $id = trim($id);
                if (empty($id)) {
                    continue;
                }
                
                $processed++;
                echo "Processing {$options['mode']} {$id} ({$processed}/" . count($ids) . ")... ";
                
                try {
                    if ($options['mode'] === 'company') {
                        $data = $crawler->fetchCompanyDetail($id);
                        if ($data) {
                            $crawler->saveCompanyDetail($id, $data);
                        }
                    } else {
                        $data = $crawler->fetchBusinessDetail($id);
                        if ($data) {
                            $crawler->saveBusinessDetail($id, $data);
                        }
                    }
                    
                    if ($data) {
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
            
            echo "\nDetail crawl results:\n";
            echo "Total processed: {$processed}\n";
            echo "Successful: {$successful}\n";
            echo "Failed: {$failed}\n";
            break;
            
        default:
            echo "Error: Unknown crawler type '{$type}'\n";
            showUsage();
    }
    
    echo "Crawl completed successfully!\n";
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}