#!/usr/bin/env php
<?php

require_once __DIR__ . '/vendor/autoload.php';

use BizData\Crawlers\CrawlerFactory;

function showUsage() {
    echo "Usage: php crawl-schools.php [options]\n";
    echo "\n";
    echo "Options:\n";
    echo "  --csv <filename>    Export results to CSV file\n";
    echo "  --category <name>   Filter by category name\n";
    echo "  --tax-id <id>       Search for specific tax ID\n";
    echo "  --help              Show this help message\n";
    echo "\n";
    echo "Examples:\n";
    echo "  php crawl-schools.php                           # Crawl all schools\n";
    echo "  php crawl-schools.php --csv schools.csv         # Export to CSV\n";
    echo "  php crawl-schools.php --category \"大學\"          # Filter by category\n";
    echo "  php crawl-schools.php --tax-id 12345678         # Search specific school\n";
    exit(1);
}

// Parse command line arguments
$options = [];
for ($i = 1; $i < $argc; $i++) {
    switch ($argv[$i]) {
        case '--help':
            showUsage();
            break;
            
        case '--csv':
            if (!isset($argv[$i + 1])) {
                echo "Error: --csv requires a filename\n";
                showUsage();
            }
            $options['csv'] = $argv[$i + 1];
            $i++; // Skip next argument
            break;
            
        case '--category':
            if (!isset($argv[$i + 1])) {
                echo "Error: --category requires a category name\n";
                showUsage();
            }
            $options['category'] = $argv[$i + 1];
            $i++; // Skip next argument
            break;
            
        case '--tax-id':
            if (!isset($argv[$i + 1])) {
                echo "Error: --tax-id requires a tax ID\n";
                showUsage();
            }
            $options['tax_id'] = $argv[$i + 1];
            $i++; // Skip next argument
            break;
            
        default:
            echo "Error: Unknown option '{$argv[$i]}'\n";
            showUsage();
    }
}

try {
    echo "Starting school data crawl\n";
    
    $factory = new CrawlerFactory([
        'log_file' => 'school_crawl_' . date('Y-m-d_H-i-s') . '.log'
    ]);
    
    $crawler = $factory->createSchoolCrawler();
    
    // Handle different modes
    if (isset($options['tax_id'])) {
        echo "Searching for school with tax ID: {$options['tax_id']}\n";
        $school = $crawler->getSchoolByTaxId($options['tax_id']);
        
        if ($school) {
            echo "Found school:\n";
            echo "  Tax ID: {$school['tax_id']}\n";
            echo "  Name: {$school['name']}\n";
            echo "  Category: {$school['category']}\n";
            echo "  Source: {$school['source']}\n";
        } else {
            echo "School not found with tax ID: {$options['tax_id']}\n";
        }
        
    } elseif (isset($options['category'])) {
        echo "Searching for schools in category: {$options['category']}\n";
        $schools = $crawler->getSchoolsByCategory($options['category']);
        
        echo "Found " . count($schools) . " schools\n";
        
        foreach ($schools as $school) {
            echo "  {$school['tax_id']} - {$school['name']}\n";
        }
        
        if (isset($options['csv'])) {
            $handle = fopen($options['csv'], 'w');
            fputcsv($handle, ['tax_id', 'name', 'category', 'source', 'type', 'crawled_at']);
            foreach ($schools as $school) {
                fputcsv($handle, [
                    $school['tax_id'],
                    $school['name'],
                    $school['category'],
                    $school['source'],
                    $school['type'],
                    $school['crawled_at']
                ]);
            }
            fclose($handle);
            echo "Results exported to: {$options['csv']}\n";
        }
        
    } else {
        echo "Crawling all schools from Ministry of Education\n";
        $schools = $crawler->crawl();
        
        echo "Crawl completed successfully!\n";
        echo "Found " . count($schools) . " schools\n";
        
        // Show sample schools
        if (!empty($schools)) {
            echo "Sample schools:\n";
            $sampleSchools = array_slice($schools, 0, 10);
            foreach ($sampleSchools as $school) {
                echo "  {$school['tax_id']} - {$school['name']} ({$school['category']})\n";
            }
            if (count($schools) > 10) {
                echo "  ... and " . (count($schools) - 10) . " more\n";
            }
        }
        
        // Export to CSV if requested
        if (isset($options['csv'])) {
            if ($crawler->exportToCSV($options['csv'])) {
                echo "Schools exported to: {$options['csv']}\n";
            } else {
                echo "Failed to export schools to CSV\n";
            }
        }
    }
    
} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
    exit(1);
}